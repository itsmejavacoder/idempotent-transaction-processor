package io.github.itsmejavacoder.txprocessor.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(info = @Info(
        title = "Idempotent Background Transaction Processing Service",
        version = "1.0.0",
        description = """
                Sequence-ordered, exactly-once transaction ingestion and processing.

                Submit transactions through a single endpoint, a JSON batch, or a file upload.
                A background worker applies contiguous global sequences to the ledger inside one
                database transaction per record. Duplicates are reported without side effects,
                gaps are visible through the sequence report, and transient failures retry safely.
                """))
public class OpenApiConfig {
}
