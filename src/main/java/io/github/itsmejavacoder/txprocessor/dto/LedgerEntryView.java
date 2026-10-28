package io.github.itsmejavacoder.txprocessor.dto;

import io.github.itsmejavacoder.txprocessor.model.TransactionType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record LedgerEntryView(
        long id,
        String transactionId,
        String accountId,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
        BigDecimal balanceAfter,
        LocalDateTime createdAt
) {
}
