package io.github.itsmejavacoder.txprocessor.dto;

import java.time.LocalDateTime;

public record TransactionEventView(
        long id,
        String transactionId,
        String status,
        String detail,
        LocalDateTime createdAt
) {
}
