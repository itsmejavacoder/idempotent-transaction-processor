package io.github.itsmejavacoder.txprocessor.dto;

import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;

public record SubmissionResponse(
        String transactionId,
        TransactionStatus status,
        String message
) {
}
