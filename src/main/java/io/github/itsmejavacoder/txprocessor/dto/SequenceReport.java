package io.github.itsmejavacoder.txprocessor.dto;

import java.util.List;

public record SequenceReport(
        long lastProcessedSequence,
        long highestObservedSequence,
        List<Long> missingSequences,
        List<PendingTransactionView> pendingTransactions
) {
}
