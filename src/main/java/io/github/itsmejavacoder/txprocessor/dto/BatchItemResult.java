package io.github.itsmejavacoder.txprocessor.dto;

public record BatchItemResult(
        int index,
        String transactionId,
        String outcome,
        String message
) {
}
