# Captured sample output

These responses were captured from a live instance started with an in-memory database and the scheduler disabled:

```bash
java -jar target/idempotent-transaction-processor-1.0.0.jar \
  --server.port=18080 \
  --processor.scheduling.enabled=false \
  --spring.datasource.url='jdbc:h2:mem:demo;DB_CLOSE_DELAY=-1'
```

Timestamps and generated IDs vary between runs.

## 1. Happy path (`samples/happy-path.json`)

Five valid records, global sequences 1–5, and a simulated transient failure on sequence 5.

### Ingest

```bash
curl.exe -s -X POST http://localhost:18080/api/transactions/upload \
  -F "file=@samples/happy-path.json"
```

```json
{
  "total": 5,
  "accepted": 5,
  "duplicates": 0,
  "rejected": 0,
  "results": [
    {"index": 0, "transactionId": "TXN-1001", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 1, "transactionId": "TXN-1002", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 2, "transactionId": "TXN-1003", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 3, "transactionId": "TXN-1004", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 4, "transactionId": "TXN-1005", "outcome": "ACCEPTED",  "message": "Accepted for background processing"}
  ]
}
```

### Processing

Pass 1 processes sequences 1–4 and schedules the simulated failure on sequence 5. Pass 2 completes the retry.

```json
{"message": "Processing pass completed", "recordsTouched": 5}
{"message": "Processing pass completed", "recordsTouched": 1}
```

### Summary

```json
{
  "generatedAt": "2026-10-28T16:00:28.853Z",
  "transactionCountsByStatus": {
    "RECEIVED": 0,
    "PROCESSING": 0,
    "PROCESSED": 5,
    "DUPLICATE": 0,
    "PENDING_SEQUENCE": 0,
    "RETRY_PENDING": 0,
    "FAILED": 0
  },
  "totalTransactions": 5,
  "duplicateRequests": 0,
  "ledgerEntries": 5
}
```

### Balances

```json
{"accountId": "ACC-301", "balance": 85.00, "lastSequence": 3, "currency": "INR"}
{"accountId": "ACC-302", "balance": 10.00, "lastSequence": 4, "currency": "INR"}
{"accountId": "ACC-303", "balance": 500.00, "lastSequence": 5, "currency": "INR"}
```

`ACC-301`: 100 credit − 40 debit + 25 credit = 85.

### Ledger

```json
[
  {"id": 1, "transactionId": "TXN-1001", "accountId": "ACC-301", "transactionType": "CREDIT", "amount": 100.00, "currency": "INR", "balanceAfter": 100.00},
  {"id": 2, "transactionId": "TXN-1002", "accountId": "ACC-301", "transactionType": "DEBIT",  "amount": 40.00,  "currency": "INR", "balanceAfter": 60.00},
  {"id": 3, "transactionId": "TXN-1003", "accountId": "ACC-301", "transactionType": "CREDIT", "amount": 25.00,  "currency": "INR", "balanceAfter": 85.00},
  {"id": 4, "transactionId": "TXN-1004", "accountId": "ACC-302", "transactionType": "CREDIT", "amount": 10.00,  "currency": "INR", "balanceAfter": 10.00},
  {"id": 5, "transactionId": "TXN-1005", "accountId": "ACC-303", "transactionType": "CREDIT", "amount": 500.00, "currency": "INR", "balanceAfter": 500.00}
]
```

### Sequence report

```json
{
  "lastProcessedSequence": 5,
  "highestObservedSequence": 5,
  "missingSequences": [],
  "pendingTransactions": []
}
```

## 2. Assessment sample (`samples/assessment-sample.json`)

The brief's suggested data: duplicate `TXN-2002`, invalid `TXN-2004` (negative amount), retry configuration on `TXN-2005`.

### Ingest

```json
{
  "total": 6,
  "accepted": 4,
  "duplicates": 1,
  "rejected": 1,
  "results": [
    {"index": 0, "transactionId": "TXN-2002", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 1, "transactionId": "TXN-2001", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 2, "transactionId": "TXN-2002", "outcome": "DUPLICATE", "message": "Duplicate request ignored; original transaction state was preserved"},
    {"index": 3, "transactionId": "TXN-2003", "outcome": "ACCEPTED",  "message": "Accepted for background processing"},
    {"index": 4, "transactionId": "TXN-2004", "outcome": "REJECTED",  "message": "amount: amount must be greater than zero"},
    {"index": 5, "transactionId": "TXN-2005", "outcome": "ACCEPTED",  "message": "Accepted for background processing"}
  ]
}
```

### Processing

```json
{"message": "Processing pass completed", "recordsTouched": 4}
```

The pass touches `TXN-2001`, `TXN-2002`, `TXN-2003` and `TXN-2005`. It cannot touch `TXN-2004` because that record was rejected before persistence.

### Summary

```json
{
  "generatedAt": "2026-10-28T16:00:54.322Z",
  "transactionCountsByStatus": {
    "RECEIVED": 0,
    "PROCESSING": 0,
    "PROCESSED": 3,
    "DUPLICATE": 1,
    "PENDING_SEQUENCE": 1,
    "RETRY_PENDING": 0,
    "FAILED": 0
  },
  "totalTransactions": 4,
  "duplicateRequests": 1,
  "ledgerEntries": 3
}
```

### Sequence report

```json
{
  "lastProcessedSequence": 3,
  "highestObservedSequence": 5,
  "missingSequences": [4],
  "pendingTransactions": [
    {
      "transactionId": "TXN-2005",
      "sequenceNumber": 5,
      "status": "PENDING_SEQUENCE",
      "lastError": "Waiting for sequence 4"
    }
  ]
}
```

`TXN-2005` waits because sequence 4 belonged to the rejected `TXN-2004`. Strict Option A does not skip a missing slot; the report makes the reason explicit. Use `happy-path.json` to see the retry path complete.

### Blocked transaction and its history

```json
{
  "transactionId": "TXN-2005",
  "requestId": "REQ-5",
  "accountId": "ACC-203",
  "transactionType": "CREDIT",
  "amount": 500.00,
  "currency": "INR",
  "sequenceNumber": 5,
  "failuresBeforeSuccess": 1,
  "attempts": 0,
  "status": "PENDING_SEQUENCE",
  "lastError": "Waiting for sequence 4"
}
```

```json
[
  {"id": 4,  "transactionId": "TXN-2005", "status": "RECEIVED",         "detail": "Accepted for background processing"},
  {"id": 11, "transactionId": "TXN-2005", "status": "PENDING_SEQUENCE", "detail": "Waiting for sequence 4"}
]
```

## 3. Duplicate behaviour

Submitting the same transaction again after processing:

```json
HTTP/1.1 200 OK

{
  "transactionId": "TXN-2001",
  "status": "DUPLICATE",
  "message": "Duplicate request ignored; original transaction state was preserved"
}
```

The ledger still has three entries, and `summary.duplicateRequests` increases by one.
