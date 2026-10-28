package io.github.itsmejavacoder.txprocessor.dto;

public record PendingTransactionView(
        String transactionId,
        long sequenceNumber,
        String status,
        String lastError
) {
}
