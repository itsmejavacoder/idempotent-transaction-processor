# Design Note — Idempotent Background Transaction Processing Service

This document explains the architecture, the correctness argument, the trade-offs, and how the implementation maps to the FDE L2 brief. It is intended to be read alongside [README.md](README.md).

## 1. Context and goals

The service receives transaction requests that may arrive out of order, may be duplicated, and may fail transiently. The non-negotiable outcome is that a unique business transaction produces at most one balance change and one ledger entry, while the system remains operable when records are missing, malformed, or permanently failing.

The brief allows a lightweight implementation. The design therefore favours a small number of explicit mechanisms that can be demonstrated and tested over a broad but shallow feature set.

## 2. Architecture

```
                 ┌──────────────────────────────────────────────┐
 HTTP clients ──►│ TransactionController                        │
 file uploads ──►│  ├─ single request (@Valid)                  │
                 │  ├─ batch / multipart → BatchIngestionService│
                 │  └─ process trigger, reads, reports          │
                 └───────────────────┬──────────────────────────┘
                                     │
                 ┌───────────────────▼──────────────────────────┐
                 │ TransactionProcessorService                  │
                 │  submit(): dedup + persist + RECEIVED event  │
                 │  processPending(): per-record transaction    │
                 └───────────────────┬──────────────────────────┘
                                     │ JDBC
                 ┌───────────────────▼──────────────────────────┐
                 │ H2 (file by default, in-memory in tests)     │
                 │ transactions · accounts · ledger_entries     │
                 │ processing_stream · duplicate_requests       │
                 │ transaction_events                           │
                 └──────────────────────────────────────────────┘

 TransactionWorker ──(fixed delay)──► processPending()
```

Input handling, business processing and scheduling are separate classes. The controller never contains ledger logic; the processor never parses HTTP payloads; the worker only triggers passes.

## 3. Data model

| Table | Role | Key constraints |
|---|---|---|
| `transactions` | Current state of every accepted request | PK `transaction_id`; unique `request_id`; unique `sequence_number` |
| `accounts` | Per-account balance and currency | PK `account_id` |
| `ledger_entries` | Immutable applied effects | Unique `transaction_id`; one row per processed transaction |
| `processing_stream` | Single-row global ordering point | PK `id`, always `1` |
| `duplicate_requests` | Append-only duplicate attempts | Indexed by transaction ID |
| `transaction_events` | Append-only status history | Indexed by transaction ID and insertion order |

`accounts.last_sequence` records the last **global** sequence applied to that account. It is informational; ordering is driven by `processing_stream`.

The unique constraints are part of the correctness argument, not just hygiene. `UNIQUE (transaction_id)` on the ledger means even a logic bug or a replay cannot produce a second posting.

## 4. Status lifecycle

```
RECEIVED ──► PENDING_SEQUENCE ──┐
    │                           │
    └────────► PROCESSING ◄─────┘
                    ├──► PROCESSED
                    ├──► RETRY_PENDING ──► PROCESSING
                    └──► FAILED

DUPLICATE ──► duplicate_requests (audit only; original transaction unchanged)
```

| Status | Meaning |
|---|---|
| `RECEIVED` | Persisted, waiting for the next pass |
| `PENDING_SEQUENCE` | Valid, but a lower sequence is missing |
| `PROCESSING` | Attempt in progress (inside the same DB transaction as the effect) |
| `RETRY_PENDING` | Simulated transient failure; will be retried |
| `PROCESSED` | Balance and ledger committed |
| `FAILED` | Terminal for this record; reason in `last_error` |
| `DUPLICATE` | Returned by the API only; the attempt is stored in `duplicate_requests` |

`PROCESSING` is written inside the record's transaction, so it is never left behind by a crash; a rollback restores the previous status. Every transition also inserts a `transaction_events` row.

## 5. Processing algorithm

For each candidate ID in sequence order:

1. `SELECT ... FOR UPDATE` the transaction row and confirm it is still in a work status.
2. Ensure the account exists, then lock it with `SELECT ... FOR UPDATE`.
3. Ensure the stream row exists, then lock it with `SELECT ... FOR UPDATE`.
4. Compare the record's sequence with `stream.last_sequence`:
   - greater → commit `PENDING_SEQUENCE` with `Waiting for sequence N`;
   - lower → commit `FAILED`, `Sequence already consumed`;
   - equal → continue.
5. Increment attempts and record `PROCESSING`.
6. If the simulated failure budget is not exhausted → `RETRY_PENDING` (or terminal `FAILED` when retries are exhausted), release locks by committing.
7. Apply the balance change, insert the ledger row, advance the stream, record `PROCESSED` — all in the same transaction.

Candidates are selected in `sequence_number, created_at` order, so one pass usually drains every contiguous record. A record that becomes eligible after a waiting record is handled on the next pass.

The worker wraps each record in its own `TransactionTemplate`. An unexpected exception rolls back only that record, is logged with its ID, and the record keeps its previous status for the next pass.

## 6. Idempotency and duplicate detection

Idempotency is layered:

1. **Pre-insert checks** on `transactionId`, then `requestId`, then `sequenceNumber`.
2. **Database uniqueness** as the authority. A race between two submitters surfaces as `DuplicateKeyException`; the service re-reads all three keys and returns the correct `DUPLICATE` or `CONFLICT` instead of retrying blindly.
3. **Ledger uniqueness** as the final defence if a transaction were ever replayed after a partial failure.
4. **Per-record transaction** so the balance, ledger and state cannot diverge.

Semantics:

| Scenario | Result |
|---|---|
| Same `transactionId`, same business payload | `DUPLICATE`, original state preserved |
| Same `transactionId`, different payload | `409 Conflict` |
| Same `requestId`, same transaction | `DUPLICATE` |
| Same `requestId`, different transaction | `409 Conflict` |
| Same sequence slot, different transaction | `409 Conflict` |

Duplicates are **persisted** in `duplicate_requests`, so the summary is correct after a restart. The original implementation counted them in an `AtomicLong`, which reset to zero on every boot.

## 7. Ordering decision

The brief offers Option A (sequence), Option B (state transitions) and Option C (latest-state). Option A was selected because it makes the ordering contract explicit and matches the suggested test data, which uses sequences 1–5 across three accounts.

**Global, not per-account.** A per-account stream would let `ACC-202` start at sequence 4 and `ACC-203` at 5, which contradicts the brief's expected outcomes. The original ChatGPT implementation did exactly that and rewrote the sample data to fit. This version keeps the brief's sample and uses one global stream.

Consequences:

- Processing is serialized on one row. This is deliberate for the assessment and is called out as the first production scaling limit.
- A permanently failed record consumes its sequence. Otherwise one insufficient-funds record would block every later record forever. The failure and reason remain in `transactions` and `transaction_events`.
- A validation-rejected record never enters the table, so its sequence becomes a real gap. `GET /api/processing/sequence-report` reports it with the blocked records and their `last_error`. It is not silently skipped.
- Sequence numbers are unique. A second transaction claiming an occupied slot is rejected at submission.

The trade-off is explicit: correctness and observability of order are prioritised over parallel throughput.

## 8. Retry and failure handling

- `failuresBeforeSuccess` simulates transient failures deterministically.
- `processor.max-retries` (default 3) caps attempts. Attempts 1–3 can be transient; attempt 4 is terminal.
- Retries occur before any ledger or balance change, so a retry cannot double-post.
- Retry exhaustion, insufficient funds and the currency backstop all mark `FAILED` and advance the stream.
- Failed records are traceable through status, attempt count, `last_error` and the event history.

**Recovery gap:** there is no automated requeue/compensation flow. Replaying a `FAILED` record needs an operator decision because its sequence slot has been consumed; simply resetting it to `RECEIVED` would hit the stale-sequence guard. A production design would use a dead-letter record, a new sequence/version, and an explicit compensating entry. This is listed as the top hardening item rather than hidden.

## 9. Consistency and concurrency

- The balance update, ledger insert, stream advance and status update are one database transaction.
- Account and stream rows are always locked in the same order, reducing deadlock risk. The transaction row is locked first.
- Waiting on a missing sequence commits quickly rather than holding locks while blocked.
- `SELECT ... FOR UPDATE` plus a single-row stream prevents two workers from applying the same slot.
- Unique constraints are the backstop against races that bypass in-memory checks.
- The concurrency test submits the same request from four threads and asserts exactly one ledger row.

H2 coordinates locks inside one JVM and on one file database. Multi-instance deployment would need a server database and a distributed claim/lease; that is out of scope.

## 10. Input handling and validation

- Single requests use Bean Validation via `@Valid`; failures produce a consistent `400` body.
- Batch and file ingestion parse each element from a `JsonNode` and validate it independently, so an unknown enum, wrong type, negative amount or duplicate does not abort the batch.
- Each element returns `ACCEPTED`, `DUPLICATE` or `REJECTED` with a reason.
- A business conflict (`409`) or currency conflict (`422`) inside a batch becomes a per-element `REJECTED`, not a batch failure.
- The batch size is capped (`processor.max-batch-size`, default 1000).
- A syntactically broken JSON document still fails as a whole with `400`; per-record isolation begins after the document parses.

## 11. Observability

| Signal | Where |
|---|---|
| Structured lifecycle logs | `transaction.accepted/processed/duplicate/retry-scheduled/failed`, `batch.ingested`, `worker.record-error` |
| Counts by status | `GET /api/transactions/summary` |
| Duplicate attempts | `duplicate_requests` + summary |
| Transition history | `GET /api/transactions/{id}/events` |
| Ordering health | `GET /api/processing/sequence-report` (stream position, gaps, blocked records) |
| Ledger audit | `GET /api/ledger` |

The summary is a processing report rather than a full metrics stack. Micrometer counters and alerting on gap age are recommended for production.

## 12. Security and data handling

- Mandatory fields, positive amounts and currency format are validated before persistence.
- Unknown JSON fields are ignored; payloads are not logged.
- No secrets or credentials are hardcoded. The H2 console is disabled unless the `dev` profile is active.
- API errors are normalised and do not expose stack traces or SQL.
- Logs contain transaction/account references, amounts and currencies — no customer PII is modelled.
- Authentication, authorization, rate limiting and TLS are deliberately out of scope and expected from the hosting platform.

## 13. Testing strategy

17 integration tests run against in-memory H2 with the scheduler disabled:

- duplicate by `transactionId` and by `requestId`, including persisted duplicate counts;
- conflicting payload and conflicting sequence slot;
- out-of-order record waiting on a predecessor owned by a different account;
- missing-sequence report and blocked record;
- transient retry and retry exhaustion;
- insufficient funds and currency mismatch;
- batch isolation of invalid, malformed and duplicate records;
- multipart file ingestion;
- consistent API error shape;
- event history;
- concurrent duplicate submissions.

Tests assert database state (balances, ledger counts, statuses) rather than only HTTP responses. The suite avoids Testcontainers because the assessment explicitly accepts lightweight local persistence; a production port would add PostgreSQL integration tests.

## 14. Trade-offs and limitations

| Decision | Benefit | Cost |
|---|---|---|
| Global sequence stream | Matches the brief and exposes gaps | Serialized processing; one hot row |
| JdbcTemplate | Explicit locks, SQL and constraints; small surface area | Manual mapping; no ORM convenience |
| File-backed H2 | Zero external dependencies; state survives restarts | Single node, not production-grade |
| Sequence-consuming failures | A bad record cannot block the stream | Replay needs a new sequence/compensation |
| Per-record transactions | Failure isolation | More commits than a bulk batch |
| Scheduler in-process | Simple demo | No distributed claim or lease |
| JSON array batch | Simple contract | A broken document fails as a whole |

These are conscious assessment trade-offs, not unnoticed gaps.

## 15. Production hardening

1. PostgreSQL + Flyway; keep every unique constraint.
2. Queue consumer (Kafka/SQS/RabbitMQ) and a transactional outbox for downstream effects.
3. Partition the stream where global order is not required; document the ordering scope per partition.
4. Distributed claim/lease with heartbeat and stale-claim recovery.
5. Dead-letter handling, audited replay/compensation and operator tooling.
6. Micrometer/Prometheus metrics, tracing, alerting on stream lag and gap age.
7. OpenAPI contract, authentication, per-client rate limits, TLS.
8. Testcontainers integration tests and a duplicate-storm load test.
9. Retention and PII policy for payload storage; encrypt sensitive fields if they are added.

## 16. AI-assisted development

The baseline was produced by ChatGPT from the brief. It was then validated line by line against the functional requirements, non-functional requirements and acceptance criteria. The review found:

- 6 tests and no batch/file ingestion;
- `requestId` persisted but never used for duplicate detection;
- per-account ordering incompatible with the brief's global sample, with the sample rewritten to fit;
- no missing-sequence reporting;
- duplicate counts held in memory;
- no transition history;
- currency validation ordered before sequence handling, allowing the stream to advance past skipped work;
- H2 console enabled by default;
- no Maven wrapper, CI, license or captured sample output.

The published version replaces the in-memory duplicate counter with a table, moves ordering to a global Option A stream, adds batch and multipart ingestion with per-record isolation, enforces `requestId`, adds the event trail and gap report, fixes the validation-order issue, adds sequence uniqueness, normalises API errors, hardens configuration, adds the wrapper/CI/license, and expands the suite to 17 tests. Each change is exercised by a test or a captured live response. `JdbcTemplate` was retained deliberately; the reasoning is in section 14 and the README.

## 17. Requirement traceability

### Functional requirements

| ID | Requirement | Implementation |
|---|---|---|
| L2-FR-01 | Configurable/replaceable input source | Single API, JSON batch, multipart file, scheduled or manual pass; input adapters are separate classes |
| L2-FR-02 | Validate and reject malformed records | Bean Validation; per-record batch outcomes; consistent `400` |
| L2-FR-03 | Maintain processing state | `transactions.status`, attempts, errors, event history |
| L2-FR-04 | Status lifecycle | `RECEIVED`, `PROCESSING`, `PROCESSED`, `DUPLICATE`, `PENDING_SEQUENCE`, `RETRY_PENDING`, `FAILED` |
| L2-FR-05 | Process each transaction once | Unique business key + ledger unique + one DB transaction per record |
| L2-FR-06 | Detect duplicates later in a batch | `transactionId` and `requestId` checks; duplicate attempts persisted |
| L2-FR-07 | Out-of-order strategy | Global sequence stream; pending state; gap report |
| L2-FR-08 | Simulate retryable failure | `failuresBeforeSuccess` |
| L2-FR-09 | Retry without duplicate effects | Retries before ledger write; ledger unique constraint |
| L2-FR-10 | Separate business logic from input | Controller → ingestion adapter → processor → worker |
| L2-FR-11 | Processing summary | Status counts, duplicates, ledger entries, generated time |
| L2-FR-12 | Tests for duplicate/out-of-order/invalid/retry | 17 integration tests including concurrency |

### Non-functional requirements

| ID | Requirement | Status |
|---|---|---|
| 6.1 Idempotency | Duplicate requests produce one outcome | Met (two keys, unique constraints, persisted duplicate audit) |
| 6.2 Ordering | Tolerate arbitrary arrival order | Met (global sequence, pending, gap report) |
| 6.3 Resilience | Transient retries; failed records traceable/recoverable | Retries and traceability met; automated recovery is a documented next step |
| 6.4 Consistency | No partial state | Met (single transaction per record) |
| 6.5 Observability | Processing visibility | Met (summary, event history, sequence report, structured logs) |
| 6.6 Security/data handling | Validate input, no secrets, safe errors, avoid sensitive logs | Met within scope; auth/TLS platform-provided |
| 6.7 Maintainability | Modular, rules separated, tests | Met at assessment scale; repository abstraction is a production refactor |

### Acceptance criteria

| Criterion | Evidence |
|---|---|
| Duplicates cause no duplicate side effects | Duplicate and concurrency tests; ledger unique constraint |
| Out-of-order handled by a defined strategy | Global Option A stream; pending and gap tests |
| Retry is safe | Retry test asserts one ledger entry and correct attempts |
| Invalid records isolated | Batch and file tests mix valid, invalid, malformed and duplicate records |
| Clear lifecycle | Status enum, transitions and event history |
| Production implications explained | README and sections 14–15 |
| AI assistance and validation explained | Section 16 |
| Trade-offs and next steps identified | Sections 14–15 |
