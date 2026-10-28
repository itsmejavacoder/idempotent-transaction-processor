package io.github.itsmejavacoder.txprocessor.dto;

import java.util.List;

public record BatchSubmissionResponse(
        int total,
        int accepted,
        int duplicates,
        int rejected,
        List<BatchItemResult> results
) {
}
