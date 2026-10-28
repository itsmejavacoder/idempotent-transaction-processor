package io.github.itsmejavacoder.txprocessor.service;

import io.github.itsmejavacoder.txprocessor.dto.AccountView;
import io.github.itsmejavacoder.txprocessor.dto.LedgerEntryView;
import io.github.itsmejavacoder.txprocessor.dto.PendingTransactionView;
import io.github.itsmejavacoder.txprocessor.dto.ProcessingSummary;
import io.github.itsmejavacoder.txprocessor.dto.SequenceReport;
import io.github.itsmejavacoder.txprocessor.dto.SubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.TransactionEventView;
import io.github.itsmejavacoder.txprocessor.dto.TransactionRequest;
import io.github.itsmejavacoder.txprocessor.dto.TransactionView;
import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;
import io.github.itsmejavacoder.txprocessor.model.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Owns the transaction state machine and the ordered, idempotent ledger update.
 *
 * <p>Ordering strategy: Option A (sequence-based, global stream). The single row in
 * {@code processing_stream} is the ordering point. A record is processed only when its
 * sequence number is exactly one greater than the stream position. Future records wait
 * in PENDING_SEQUENCE; missing numbers are surfaced by {@link #getSequenceReport()}.</p>
 */
@Service
public class TransactionProcessorService {
    private static final Logger log = LoggerFactory.getLogger(TransactionProcessorService.class);
    private static final int MAX_MISSING_SEQUENCES = 200;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactionTemplate;
    private final int maxRetries;

    public TransactionProcessorService(
            JdbcTemplate jdbc,
            PlatformTransactionManager transactionManager,
            @Value("${processor.max-retries:3}") int maxRetries) {
        this.jdbc = jdbc;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.maxRetries = maxRetries;
    }

    /**
     * Persists a new request or resolves it as a duplicate. Duplicate checks are done on
     * both the business key ({@code transactionId}) and the retry key ({@code requestId}).
     */
    public SubmissionResponse submit(TransactionRequest request) {
        rejectAccountCurrencyConflict(request);

        TransactionView byTransactionId = findByTransactionId(request.transactionId());
        if (byTransactionId != null) {
            if (!sameBusinessRequest(byTransactionId, request)) {
                throw conflict("transactionId already exists with a different payload");
            }
            return recordDuplicate(request, byTransactionId.transactionId(), "Duplicate transactionId");
        }

        TransactionView byRequestId = findByRequestId(request.requestId());
        if (byRequestId != null) {
            if (byRequestId.transactionId().equals(request.transactionId())) {
                return recordDuplicate(request, byRequestId.transactionId(), "Duplicate requestId");
            }
            throw conflict("requestId already belongs to transaction " + byRequestId.transactionId());
        }

        try {
            transactionTemplate.executeWithoutResult(status -> {
                jdbc.update("""
                        INSERT INTO transactions
                        (transaction_id, request_id, account_id, transaction_type, amount, currency,
                         sequence_number, failures_before_success, attempts, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        request.transactionId(), request.requestId(), request.accountId(),
                        request.transactionType().name(), request.amount(), request.currency(),
                        request.sequenceNumber(), request.failuresBeforeSuccess(),
                        TransactionStatus.RECEIVED.name());
                recordEvent(request.transactionId(), TransactionStatus.RECEIVED,
                        "Accepted for background processing");
            });
        } catch (DuplicateKeyException ex) {
            return resolveInsertRace(request);
        }

        log.info("transaction.accepted transactionId={} requestId={} accountId={} sequence={}",
                request.transactionId(), request.requestId(), request.accountId(), request.sequenceNumber());
        return new SubmissionResponse(request.transactionId(), TransactionStatus.RECEIVED,
                "Accepted for background processing");
    }

    /**
     * Called by the scheduled worker or POST /api/transactions/process. Each record runs in
     * its own transaction so one bad record cannot roll back unrelated work.
     */
    public int processPendingTransactions() {
        ensureProcessingStream();
        List<String> ids = jdbc.queryForList("""
                SELECT transaction_id FROM transactions
                WHERE status IN (?, ?, ?)
                ORDER BY sequence_number, created_at
                """, String.class,
                TransactionStatus.RECEIVED.name(),
                TransactionStatus.PENDING_SEQUENCE.name(),
                TransactionStatus.RETRY_PENDING.name());

        int touched = 0;
        for (String id : ids) {
            try {
                if (Boolean.TRUE.equals(transactionTemplate.execute(status -> processOne(id)))) {
                    touched++;
                }
            } catch (RuntimeException ex) {
                // The record keeps its previous status and is retried on the next pass.
                log.error("worker.record-error transactionId={} error={}", id, ex.getMessage(), ex);
            }
        }
        return touched;
    }

    private boolean processOne(String transactionId) {
        List<TransactionView> rows = jdbc.query(
                "SELECT * FROM transactions WHERE transaction_id = ? FOR UPDATE",
                transactionMapper, transactionId);
        if (rows.isEmpty()) {
            return false;
        }
        TransactionView tx = rows.get(0);
        if (!(tx.status() == TransactionStatus.RECEIVED
                || tx.status() == TransactionStatus.PENDING_SEQUENCE
                || tx.status() == TransactionStatus.RETRY_PENDING)) {
            return false;
        }

        ensureProcessingStream();
        ensureAccount(tx.accountId(), tx.currency());

        AccountView account = jdbc.queryForObject("""
                SELECT account_id, balance, last_sequence, currency
                FROM accounts WHERE account_id = ? FOR UPDATE
                """, accountMapper, tx.accountId());
        Long streamValue = jdbc.queryForObject(
                "SELECT last_sequence FROM processing_stream WHERE id = 1 FOR UPDATE", Long.class);
        long streamPosition = streamValue == null ? 0L : streamValue;

        if (!account.currency().equals(tx.currency())) {
            failAndConsume(tx, "Account currency mismatch", tx.attempts());
            return true;
        }

        long expected = streamPosition + 1;
        if (tx.sequenceNumber() > expected) {
            updateStatus(tx.transactionId(), TransactionStatus.PENDING_SEQUENCE,
                    "Waiting for sequence " + expected, tx.attempts());
            log.info("transaction.pending-sequence transactionId={} expected={} received={}",
                    tx.transactionId(), expected, tx.sequenceNumber());
            return true;
        }
        if (tx.sequenceNumber() < expected) {
            updateStatus(tx.transactionId(), TransactionStatus.FAILED,
                    "Sequence already consumed; possible conflicting transaction", tx.attempts());
            log.warn("transaction.stale-sequence transactionId={} expected={} received={}",
                    tx.transactionId(), expected, tx.sequenceNumber());
            return true;
        }

        int attempts = tx.attempts() + 1;
        updateStatus(tx.transactionId(), TransactionStatus.PROCESSING, null, attempts);

        if (attempts <= tx.failuresBeforeSuccess()) {
            if (attempts >= maxRetries + 1) {
                failAndConsume(tx, "Retry limit exhausted after simulated transient failures", attempts);
            } else {
                updateStatus(tx.transactionId(), TransactionStatus.RETRY_PENDING,
                        "Simulated transient failure; will retry", attempts);
                log.warn("transaction.retry-scheduled transactionId={} attempt={}", tx.transactionId(), attempts);
            }
            return true;
        }

        BigDecimal newBalance = account.balance();
        if (tx.transactionType() == TransactionType.CREDIT) {
            newBalance = newBalance.add(tx.amount());
        } else {
            if (account.balance().compareTo(tx.amount()) < 0) {
                failAndConsume(tx, "Insufficient funds", attempts);
                return true;
            }
            newBalance = newBalance.subtract(tx.amount());
        }

        jdbc.update("UPDATE accounts SET balance=?, last_sequence=? WHERE account_id=?",
                newBalance, tx.sequenceNumber(), tx.accountId());

        // The unique constraint on ledger_entries.transaction_id is the second line of defense.
        jdbc.update("""
                INSERT INTO ledger_entries
                (transaction_id, account_id, transaction_type, amount, currency, balance_after, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, tx.transactionId(), tx.accountId(), tx.transactionType().name(),
                tx.amount(), tx.currency(), newBalance);

        advanceStream(tx.sequenceNumber());
        updateStatus(tx.transactionId(), TransactionStatus.PROCESSED, null, attempts);
        log.info("transaction.processed transactionId={} accountId={} sequence={} amount={} currency={}",
                tx.transactionId(), tx.accountId(), tx.sequenceNumber(), tx.amount(), tx.currency());
        return true;
    }

    private void rejectAccountCurrencyConflict(TransactionRequest request) {
        List<AccountView> accounts = jdbc.query("""
                SELECT account_id, balance, last_sequence, currency FROM accounts WHERE account_id=?
                """, accountMapper, request.accountId());
        if (!accounts.isEmpty() && !accounts.get(0).currency().equals(request.currency())) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "Account " + request.accountId() + " uses currency "
                            + accounts.get(0).currency() + ", not " + request.currency());
        }
    }

    private SubmissionResponse resolveInsertRace(TransactionRequest request) {
        TransactionView byTransactionId = findByTransactionId(request.transactionId());
        if (byTransactionId != null) {
            if (!sameBusinessRequest(byTransactionId, request)) {
                throw conflict("transactionId already exists with a different payload");
            }
            return recordDuplicate(request, byTransactionId.transactionId(), "Duplicate transactionId (concurrent submit)");
        }

        TransactionView byRequestId = findByRequestId(request.requestId());
        if (byRequestId != null) {
            if (byRequestId.transactionId().equals(request.transactionId())) {
                return recordDuplicate(request, byRequestId.transactionId(), "Duplicate requestId (concurrent submit)");
            }
            throw conflict("requestId already belongs to transaction " + byRequestId.transactionId());
        }

        TransactionView bySequence = findBySequence(request.sequenceNumber());
        if (bySequence != null) {
            throw conflict("sequenceNumber " + request.sequenceNumber()
                    + " is already assigned to transaction " + bySequence.transactionId());
        }
        throw conflict("Concurrent submission conflicted with an existing record");
    }

    private SubmissionResponse recordDuplicate(TransactionRequest request, String originalTransactionId, String reason) {
        jdbc.update("""
                INSERT INTO duplicate_requests (transaction_id, request_id, reason) VALUES (?, ?, ?)
                """, request.transactionId(), request.requestId(), reason);
        log.info("transaction.duplicate transactionId={} requestId={} originalTransactionId={} reason={}",
                request.transactionId(), request.requestId(), originalTransactionId, reason);
        return new SubmissionResponse(originalTransactionId, TransactionStatus.DUPLICATE,
                "Duplicate request ignored; original transaction state was preserved");
    }

    private boolean sameBusinessRequest(TransactionView old, TransactionRequest request) {
        return old.accountId().equals(request.accountId())
                && old.transactionType() == request.transactionType()
                && old.amount().compareTo(request.amount()) == 0
                && old.currency().equals(request.currency())
                && old.sequenceNumber() == request.sequenceNumber();
    }

    private TransactionView findByTransactionId(String transactionId) {
        List<TransactionView> rows = jdbc.query(
                "SELECT * FROM transactions WHERE transaction_id = ?", transactionMapper, transactionId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private TransactionView findByRequestId(String requestId) {
        List<TransactionView> rows = jdbc.query(
                "SELECT * FROM transactions WHERE request_id = ?", transactionMapper, requestId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private TransactionView findBySequence(long sequenceNumber) {
        List<TransactionView> rows = jdbc.query(
                "SELECT * FROM transactions WHERE sequence_number = ?", transactionMapper, sequenceNumber);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void ensureAccount(String accountId, String currency) {
        try {
            jdbc.update("INSERT INTO accounts(account_id, balance, last_sequence, currency) VALUES (?, 0, 0, ?)",
                    accountId, currency);
        } catch (DuplicateKeyException ignored) {
            // Account already exists.
        }
    }

    private void ensureProcessingStream() {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM processing_stream WHERE id = 1", Integer.class);
        if (count != null && count == 0) {
            try {
                jdbc.update("INSERT INTO processing_stream(id, last_sequence) VALUES (1, 0)");
            } catch (DuplicateKeyException ignored) {
                // Another worker created the stream row.
            }
        }
    }

    private void advanceStream(long sequenceNumber) {
        jdbc.update("UPDATE processing_stream SET last_sequence=? WHERE id=1 AND last_sequence < ?",
                sequenceNumber, sequenceNumber);
    }

    private void failAndConsume(TransactionView tx, String reason, int attempts) {
        // A permanently failed record must not block the rest of the stream. Rejected
        // validation records never enter the stream and are reported as gaps instead.
        advanceStream(tx.sequenceNumber());
        updateStatus(tx.transactionId(), TransactionStatus.FAILED, reason, attempts);
        log.error("transaction.failed transactionId={} sequence={} reason={}",
                tx.transactionId(), tx.sequenceNumber(), reason);
    }

    private void updateStatus(String id, TransactionStatus status, String error, int attempts) {
        jdbc.update("""
                UPDATE transactions SET status=?, last_error=?, attempts=?, updated_at=CURRENT_TIMESTAMP
                WHERE transaction_id=?
                """, status.name(), error, attempts, id);
        recordEvent(id, status, error);
    }

    private void recordEvent(String transactionId, TransactionStatus status, String detail) {
        jdbc.update("""
                INSERT INTO transaction_events (transaction_id, status, detail) VALUES (?, ?, ?)
                """, transactionId, status.name(), detail);
    }

    public TransactionView getTransaction(String transactionId) {
        TransactionView tx = findByTransactionId(transactionId);
        if (tx == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Transaction not found");
        }
        return tx;
    }

    public AccountView getAccount(String accountId) {
        List<AccountView> rows = jdbc.query("""
                SELECT account_id, balance, last_sequence, currency FROM accounts WHERE account_id=?
                """, accountMapper, accountId);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found");
        }
        return rows.get(0);
    }

    public List<LedgerEntryView> getLedger() {
        return jdbc.query("""
                SELECT id, transaction_id, account_id, transaction_type, amount, currency, balance_after, created_at
                FROM ledger_entries ORDER BY id
                """, ledgerMapper);
    }

    public List<TransactionEventView> getEvents(String transactionId) {
        getTransaction(transactionId);
        return jdbc.query("""
                SELECT id, transaction_id, status, detail, created_at
                FROM transaction_events WHERE transaction_id=? ORDER BY id
                """, (rs, rowNum) -> new TransactionEventView(
                rs.getLong("id"), rs.getString("transaction_id"), rs.getString("status"),
                rs.getString("detail"), rs.getTimestamp("created_at").toLocalDateTime()), transactionId);
    }

    public ProcessingSummary getSummary() {
        long duplicateCount = count("SELECT COUNT(*) FROM duplicate_requests");
        Map<String, Long> counts = new LinkedHashMap<>();
        long total = 0;
        for (TransactionStatus status : TransactionStatus.values()) {
            if (status == TransactionStatus.DUPLICATE) {
                counts.put(status.name(), duplicateCount);
                continue;
            }
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM transactions WHERE status=?", Long.class, status.name());
            long value = count == null ? 0L : count;
            counts.put(status.name(), value);
            total += value;
        }
        return new ProcessingSummary(Instant.now(), counts, total, duplicateCount,
                count("SELECT COUNT(*) FROM ledger_entries"));
    }

    /**
     * Reports the stream position, missing numbers and records that are still waiting.
     * The report is intentionally read-only and safe to expose to operators.
     */
    public SequenceReport getSequenceReport() {
        ensureProcessingStream();
        Long stream = jdbc.queryForObject(
                "SELECT last_sequence FROM processing_stream WHERE id=1", Long.class);
        long lastProcessed = stream == null ? 0L : stream;

        Long maxSequence = jdbc.queryForObject(
                "SELECT COALESCE(MAX(sequence_number), 0) FROM transactions", Long.class);
        long highestObserved = Math.max(lastProcessed, maxSequence == null ? 0L : maxSequence);

        Set<Long> present = new HashSet<>(jdbc.queryForList(
                "SELECT sequence_number FROM transactions", Long.class));
        List<Long> missing = new ArrayList<>();
        for (long sequence = 1; sequence <= highestObserved && missing.size() < MAX_MISSING_SEQUENCES; sequence++) {
            if (!present.contains(sequence)) {
                missing.add(sequence);
            }
        }

        List<PendingTransactionView> pending = jdbc.query("""
                SELECT transaction_id, sequence_number, status, last_error FROM transactions
                WHERE status IN (?, ?, ?) ORDER BY sequence_number
                """, (rs, rowNum) -> new PendingTransactionView(
                rs.getString("transaction_id"), rs.getLong("sequence_number"),
                rs.getString("status"), rs.getString("last_error")),
                TransactionStatus.RECEIVED.name(),
                TransactionStatus.PENDING_SEQUENCE.name(),
                TransactionStatus.RETRY_PENDING.name());

        return new SequenceReport(lastProcessed, highestObserved, missing, pending);
    }

    private long count(String sql) {
        Long value = jdbc.queryForObject(sql, Long.class);
        return value == null ? 0L : value;
    }

    private ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }

    private final RowMapper<TransactionView> transactionMapper = (rs, rowNum) -> mapTransaction(rs);

    private TransactionView mapTransaction(ResultSet rs) throws SQLException {
        return new TransactionView(
                rs.getString("transaction_id"),
                rs.getString("request_id"),
                rs.getString("account_id"),
                TransactionType.valueOf(rs.getString("transaction_type")),
                rs.getBigDecimal("amount"),
                rs.getString("currency"),
                rs.getLong("sequence_number"),
                rs.getInt("failures_before_success"),
                rs.getInt("attempts"),
                TransactionStatus.valueOf(rs.getString("status")),
                rs.getString("last_error"),
                rs.getTimestamp("created_at").toLocalDateTime(),
                rs.getTimestamp("updated_at").toLocalDateTime());
    }

    private final RowMapper<AccountView> accountMapper = (rs, rowNum) -> new AccountView(
            rs.getString("account_id"), rs.getBigDecimal("balance"),
            rs.getLong("last_sequence"), rs.getString("currency"));

    private final RowMapper<LedgerEntryView> ledgerMapper = (rs, rowNum) -> new LedgerEntryView(
            rs.getLong("id"), rs.getString("transaction_id"), rs.getString("account_id"),
            TransactionType.valueOf(rs.getString("transaction_type")), rs.getBigDecimal("amount"),
            rs.getString("currency"), rs.getBigDecimal("balance_after"),
            rs.getTimestamp("created_at").toLocalDateTime());
}
