package io.github.itsmejavacoder.txprocessor.dto;

import java.math.BigDecimal;

public record AccountView(String accountId, BigDecimal balance, long lastSequence, String currency) {
}
