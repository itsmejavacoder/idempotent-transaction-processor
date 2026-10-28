package io.github.itsmejavacoder.txprocessor.dto;

import java.time.Instant;
import java.util.Map;

public record ProcessingSummary(
        Instant generatedAt,
        Map<String, Long> transactionCountsByStatus,
        long totalTransactions,
        long duplicateRequests,
        long ledgerEntries
) {
}
