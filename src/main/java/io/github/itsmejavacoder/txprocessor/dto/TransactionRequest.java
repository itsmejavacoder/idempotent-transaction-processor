package io.github.itsmejavacoder.txprocessor.dto;

import io.github.itsmejavacoder.txprocessor.model.TransactionType;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record TransactionRequest(
        @NotBlank @Size(max = 100) String transactionId,
        @NotBlank @Size(max = 100) String requestId,
        @NotBlank @Size(max = 100) String accountId,
        @NotNull TransactionType transactionType,
        @NotNull @DecimalMin(value = "0.01", message = "amount must be greater than zero")
        BigDecimal amount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}", message = "currency must be a 3-letter uppercase code")
        String currency,
        @Min(value = 1, message = "sequenceNumber must be at least 1") long sequenceNumber,
        @Min(value = 0, message = "failuresBeforeSuccess must be zero or greater")
        int failuresBeforeSuccess
) {
}
