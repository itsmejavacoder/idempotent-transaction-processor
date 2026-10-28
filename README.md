# Idempotent Background Transaction Processing Service

[![ci](https://github.com/itsmejavacoder/idempotent-transaction-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/itsmejavacoder/idempotent-transaction-processor/actions/workflows/ci.yml)

A Spring Boot 3 / Java 21 background service that ingests transaction requests from an API, a JSON batch, or an uploaded file and applies them to a ledger exactly once — even when requests arrive out of order, are duplicated, or fail and retry.

The service is a small state machine around one ordered ledger. Requests are persisted first, then a worker applies them in sequence inside a single database transaction per record. The full request-to-ledger path, including duplicate, gap, retry and restart behaviour, is documented in [End-to-end flows](DESIGN.md#16-end-to-end-flows).

## What it guarantees

| Guarantee | Mechanism |
|---|---|
| A transaction is never posted twice | `transactions.transaction_id` primary key + unique `ledger_entries.transaction_id` + per-record DB transaction |
| A retried request cannot double-post | `request_id` uniqueness and duplicate detection before insert |
| No partial state | Balance update, ledger insert, stream advance and status change commit together or roll back together |
| Out-of-order events wait, they do not corrupt | Single `processing_stream` row; a record runs only when its sequence is exactly `last_sequence + 1` |
| Missing events are visible | `GET /api/processing/sequence-report` lists gaps and blocked records |
| One bad record does not abort a batch | Parsing and Bean Validation run per record; each outcome is reported independently |
| Failures are traceable | Append-only `transaction_events` history plus status, attempt count and `last_error` |
| Transient failures retry safely | Bounded retries with a deterministic failure simulator; the ledger is only written on success |

## End-to-end flow

```text
 Client / file upload
          │
          ▼
 ┌───────────────────────────────┐
 │ TransactionController         │
 │  POST /transactions           │
 │  POST /transactions/batch     │
 │  POST /transactions/upload    │
 └──────────────┬────────────────┘
                │ single: @Valid
                │ batch/file: parse + validate each record
                ▼
 ┌───────────────────────────────┐
 │ TransactionProcessorService   │
 │  duplicate checks             │
 │  INSERT RECEIVED + event      │
 └──────────────┬────────────────┘
                ▼
        transactions (H2)
                │
                │  every second, or POST /transactions/process
                ▼
 ┌───────────────────────────────┐
 │ TransactionWorker             │
 │  processPendingTransactions() │
 └──────────────┬────────────────┘
                │ per record, per DB transaction
                ▼
 ┌───────────────────────────────┐
 │ Claim + sequence gate         │
 │  lock transaction             │
 │  lock account                 │
 │  lock processing_stream       │
 └───────┬───────────────┬───────┘
         │               │
   gap / future      sequence matches
         │               │
         ▼               ▼
  PENDING_SEQUENCE   apply balance + ledger
                         + stream + PROCESSED
                         (one commit)
         │
         ├── transient failure ──► RETRY_PENDING ──► next pass
         └── terminal failure ───► FAILED + stream advance
```

| Step | What happens | Where |
|---|---|---|
| 1. Ingress | A single request, JSON array, multipart file or manual trigger enters the API | `TransactionController` |
| 2. Validation | Single requests use Bean Validation; each batch element is parsed and validated independently | `BatchIngestionService` |
| 3. Idempotency | `transactionId` and `requestId` are checked; duplicates are recorded, not re-applied | `TransactionProcessorService.submit` |
| 4. Durable state | The request is stored as `RECEIVED` with a `RECEIVED` event before any processing | `transactions`, `transaction_events` |
| 5. Discovery | The scheduler polls work statuses in sequence order, or an operator triggers a pass | `TransactionWorker`, `processPending` endpoint |
| 6. Ordering | The transaction, account and stream rows are locked; a future sequence waits in `PENDING_SEQUENCE` | `processOne` |
| 7. Application | Balance, ledger, stream position and `PROCESSED` status commit together | `accounts`, `ledger_entries`, `processing_stream` |
| 8. Reporting | Status counts, gaps, blocked records, events and ledger are queryable | `/summary`, `/sequence-report`, `/events`, `/ledger` |

Failure, duplicate, gap and restart paths are detailed in [DESIGN.md §16](DESIGN.md#16-end-to-end-flows).

## Quick start

Requirements: JDK 21. Maven is not required — the wrapper is included.

```bash
./mvnw test          # 17 integration tests
./mvnw spring-boot:run
```

Windows:

```powershell
.\mvnw.cmd test
.\mvnw.cmd spring-boot:run
```

The service starts on `http://localhost:8080`. It uses a file-backed H2 database at `./data/transactiondb`; delete `./data` to reset local state. The scheduler processes pending records every second.

H2 console is **disabled by default**. For local debugging:

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
# http://localhost:8080/h2-console  (JDBC: jdbc:h2:file:./data/transactiondb, user sa, empty password)
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/transactions` | Submit one transaction (`202`, or `200` for a duplicate) |
| `POST` | `/api/transactions/batch` | Submit a JSON array, or `{"transactions":[...]}` |
| `POST` | `/api/transactions/upload` | Upload a JSON file (`multipart/form-data`, field `file`) |
| `POST` | `/api/transactions/process` | Run one processing pass manually |
| `GET` | `/api/transactions/{id}` | Transaction state |
| `GET` | `/api/transactions/{id}/events` | Status-transition history |
| `GET` | `/api/transactions/summary` | Counts by status, duplicates and ledger entries |
| `GET` | `/api/processing/sequence-report` | Stream position, missing sequences, blocked records |
| `GET` | `/api/accounts/{id}` | Balance, currency and last applied sequence |
| `GET` | `/api/ledger` | Ordered ledger entries |

### Submit one transaction

```bash
curl.exe -s -X POST http://localhost:8080/api/transactions \
  -H "Content-Type: application/json" \
  --data-binary @samples/single-transaction.json
```

Batch input (the assessment sample is an array):

```bash
curl.exe -s -X POST http://localhost:8080/api/transactions/batch \
  -H "Content-Type: application/json" \
  --data-binary @samples/assessment-sample.json
```

File upload:

```bash
curl.exe -s -X POST http://localhost:8080/api/transactions/upload \
  -F "file=@samples/happy-path.json"
```

Single-record body:

```json
{
  "transactionId": "TXN-1001",
  "requestId": "REQ-1001",
  "accountId": "ACC-301",
  "transactionType": "CREDIT",
  "amount": 100.00,
  "currency": "INR",
  "sequenceNumber": 1,
  "failuresBeforeSuccess": 0
}
```

`failuresBeforeSuccess` is a deliberate demo knob: `1` means the first processing attempt fails with a simulated transient error, then the retry succeeds. A value greater than `processor.max-retries` exercises retry exhaustion.

### Drive processing manually

The scheduler is on by default. To control passes step by step:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--processor.scheduling.enabled=false

curl.exe -s -X POST http://localhost:8080/api/transactions/process
curl.exe -s http://localhost:8080/api/transactions/summary
curl.exe -s http://localhost:8080/api/processing/sequence-report
```

`recordsTouched` counts every record the pass advanced, including records that moved to `PENDING_SEQUENCE` or `RETRY_PENDING`.

### Error shape

All controller errors use one body:

```json
{
  "timestamp": "2026-10-28T16:00:54.322Z",
  "status": 400,
  "error": "Bad Request",
  "message": "amount: amount must be greater than zero"
}
```

## Sample runs

Two inputs are provided, and both were executed against a live instance. Full responses are in [samples/expected-output.md](samples/expected-output.md).

### `samples/happy-path.json` — global sequences 1–5, one retry

- Batch: `5` total, `5` accepted, `0` rejected.
- Pass 1: sequences 1–4 processed; sequence 5 enters `RETRY_PENDING`.
- Pass 2: sequence 5 is processed.
- Final: `5` processed, `5` ledger entries, no gaps; account balances `85`, `10`, and `500` INR.

### `samples/assessment-sample.json` — the brief's suggested data

- Batch: `6` total, `4` accepted, `1` duplicate (`TXN-2002` / `REQ-2-DUP`), `1` rejected (`TXN-2004`, negative amount).
- Pass 1: `TXN-2001`, `TXN-2002`, `TXN-2003` processed.
- `TXN-2005` stays `PENDING_SEQUENCE` because sequence `4` belonged to the rejected record.
- Sequence report: `lastProcessedSequence: 3`, `missingSequences: [4]`, one blocked record.

This is intentional and is the honest behaviour of strict sequence-based ordering: a rejected record creates a real gap, so later records wait instead of silently skipping it. Use `happy-path.json` to see the retry path complete.

## Ordering strategy — Option A, global stream

The brief offers three strategies. This implementation uses **Option A: sequence-based processing** with one global stream.

- Every accepted record has a unique `sequenceNumber` (`UNIQUE` constraint).
- `processing_stream.last_sequence` is the ordering point and is locked with `SELECT ... FOR UPDATE`.
- `sequence > last + 1` → `PENDING_SEQUENCE`, with `last_error` naming the sequence it waits for.
- `sequence == last + 1` → process.
- `sequence < last + 1` → `FAILED` (`Sequence already consumed`), because a different record already used that slot.

**Why global instead of per-account?** The assessment's suggested data uses sequences 1–5 across three different accounts, and its expected outcome treats sequence 1 as the predecessor of sequence 2 regardless of account. A per-account stream cannot run that sample and quietly requires rewriting the input. A global stream matches the brief and makes gaps observable. The trade-off is that processing is serialized: one record at a time. That is acceptable for this assessment and is the first thing to shard in production (for example, one stream per partition key with a documented ordering scope).

**What consumes a sequence?**

- Successful processing: yes.
- Permanent business failure after validation (insufficient funds, retry exhaustion, currency mismatch backstop): yes — otherwise one bad record would block the stream forever. The record is `FAILED` and the reason is retained.
- Rejected validation (bad amount, unknown type, malformed JSON): no — it never enters the table, so it shows up as a missing sequence instead. The gap report is the operational signal.

## Idempotency and duplicate detection

Two keys are enforced:

1. **`transactionId`** — the business key. Submitting the same ID again returns `DUPLICATE` and leaves the original row untouched. A different payload for the same ID returns `409`.
2. **`requestId`** — the retry/idempotency key. Reusing it for a different transaction returns `409`; reusing it for the same transaction returns `DUPLICATE`.

Duplicate attempts are appended to `duplicate_requests`, so `summary.duplicateRequests` and `countsByStatus.DUPLICATE` survive restarts. Inserts still race safely: a `DuplicateKeyException` is resolved by re-reading both keys and the sequence slot instead of blindly retrying.

## Status lifecycle

```
RECEIVED ──► PENDING_SEQUENCE ──┐
    │                           │
    └────────► PROCESSING ◄─────┘
                    ├──► PROCESSED
                    ├──► RETRY_PENDING ──► PROCESSING
                    └──► FAILED

DUPLICATE ──► duplicate_requests (audit); the original transaction is unchanged
```

Every transition is also written to `transaction_events`, including the failure reason, and is available at `/api/transactions/{id}/events`.

## Resilience and consistency

- Each record is processed in its own `TransactionTemplate`, so one failure cannot roll back unrelated records.
- Account and stream rows are locked in a consistent order (account, then stream) to reduce deadlock risk.
- The ledger insert and balance update share the record's transaction. A crash rolls the whole record back; the next pass retries it.
- The worker catches per-record exceptions and logs them; the record keeps its previous status.
- Retry attempts are counted on the transaction row and capped by `processor.max-retries` (default `3`, so attempt 4 is the terminal failure).
- Retries happen before any ledger write, and the unique ledger constraint is the final defence if anything is replayed.

## Observability

- Structured key/value logs: `transaction.accepted`, `transaction.processed`, `transaction.duplicate`, `transaction.retry-scheduled`, `transaction.failed`, `batch.ingested`, `worker.record-error`.
- `GET /api/transactions/summary` — counts by status, total transactions, duplicate attempts, ledger entries, generation time.
- `GET /api/processing/sequence-report` — stream position, highest observed sequence, missing numbers (capped at 200), blocked records with reasons.
- `GET /api/transactions/{id}/events` — full transition history.

No customer PII is modelled or logged; logs contain transaction/account references, amounts, currencies and reasons. Request bodies are never logged.

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `server.port` | `8080` | HTTP port |
| `spring.datasource.url` | file H2 at `./data/transactiondb` | Persistence |
| `processor.max-retries` | `3` | Transient attempts before `FAILED` |
| `processor.poll-interval-ms` | `1000` | Scheduler delay between passes |
| `processor.max-batch-size` | `1000` | Maximum records per batch/file |
| `processor.scheduling.enabled` | `true` | Enables the background worker |
| `spring.h2.console.enabled` | `false` | Debug console (enabled by the `dev` profile) |

No secrets are required or hardcoded.

## Tests

`./mvnw test` runs 17 integration tests against H2:

- duplicate `transactionId` → one ledger entry, duplicate counted persistently
- duplicate `requestId` for a different transaction → rejected
- same `transactionId` with a different payload → conflict
- duplicate sequence number → rejected
- out-of-order record waiting on a predecessor owned by another account (proves the stream is global)
- missing-sequence report and blocked record
- transient failure retry without duplicate effect
- retry exhaustion → `FAILED`, stream unblocked
- insufficient funds → `FAILED`, stream unblocked
- currency mismatch once an account exists
- batch isolation of invalid and duplicate records
- malformed enum in a batch record isolated
- multipart file ingestion with per-record outcomes
- consistent API validation error body and a consistent `404` for unknown routes
- lifecycle events recorded
- concurrent duplicate submissions → one ledger entry

## Why JdbcTemplate instead of JPA?

Both use the same JDBC connection and the same underlying commit/rollback semantics; `JpaTransactionManager` does not make transactions “more transactional”. This service is a small state machine over six flat tables, and its correctness depends on explicit row locks, conditional updates and unique constraints. `JdbcTemplate` keeps those visible in the SQL:

- `SELECT ... FOR UPDATE` shows exactly which rows are claimed.
- `UPDATE processing_stream ... WHERE last_sequence < ?` is one atomic conditional advance.
- Unique constraints are the idempotency backstop; a duplicate-key failure is caught and resolved deterministically.
- There is no persistence context, dirty-checking flush ordering, or entity state to reason about after a constraint violation.

JPA would be a reasonable choice for a richer domain or CRUD-heavy API. Here it would add mapping, flush and lock-management complexity without changing any guarantee. See [DESIGN.md](DESIGN.md) for the full trade-off.

## Known limitations

- The global stream serializes processing; throughput is one record per transaction pass.
- H2 file storage and the in-process scheduler are single-node. Do not run multiple instances against the same file database.
- A permanently failed transaction consumes its sequence. There is no automated compensation/replay flow; the record and its reason are retained, but replay needs a new sequence or an operator procedure.
- A JSON batch must be syntactically valid as a whole. Per-record isolation covers schema/validation problems, not a broken array.
- No authentication, authorization, rate limiting or TLS termination — expected to be provided by the platform.
- No Micrometer/Prometheus endpoint; the summary, event history and structured logs are the observability surface.

## Production hardening

1. PostgreSQL (or another server DB) with Flyway migrations and the same unique constraints.
2. Replace the scheduler with a queue consumer (Kafka/SQS) and a transactional outbox; keep the idempotency keys.
3. Shard the stream by account/partition once global ordering is no longer required; document the ordering scope.
4. Distributed claim/lease with heartbeat instead of a single-row lock; add stale-claim recovery.
5. Dead-letter queue and an explicit, audited replay/compensation procedure for `FAILED` records.
6. Micrometer metrics (`processed`, `duplicate`, `failed`, `retry`, `pending`, stream lag) and alerting on gap age.
7. OpenAPI contract, contract tests, Testcontainers integration tests, and a load test for duplicate storms.
8. Payload encryption/retention policy and PII classification; authentication and per-client rate limits.

## Project layout

```
src/main/java/io/github/itsmejavacoder/txprocessor
├── controller/   REST API, validation error mapping
├── dto/          request/response records
├── model/        status and type enums
├── service/      processor, batch/file ingestion
└── worker/       scheduled poller
src/main/resources
├── schema.sql    tables, indexes, unique constraints
├── application.yml
└── application-dev.yml
samples/          sample inputs and captured output
```

## License

MIT — see [LICENSE](LICENSE).
