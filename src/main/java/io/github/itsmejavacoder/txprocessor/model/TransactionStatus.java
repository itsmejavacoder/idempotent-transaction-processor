package io.github.itsmejavacoder.txprocessor.model;

public enum TransactionStatus {
    RECEIVED,
    PROCESSING,
    PROCESSED,
    DUPLICATE,
    PENDING_SEQUENCE,
    RETRY_PENDING,
    FAILED
}
