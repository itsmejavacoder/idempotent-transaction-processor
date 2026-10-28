package io.github.itsmejavacoder.txprocessor.dto;

import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;
import io.github.itsmejavacoder.txprocessor.model.TransactionType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionView(
        String transactionId,
        String requestId,
        String accountId,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
        long sequenceNumber,
        int failuresBeforeSuccess,
        int attempts,
        TransactionStatus status,
        String lastError,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
