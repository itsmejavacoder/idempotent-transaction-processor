package io.github.itsmejavacoder.txprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.itsmejavacoder.txprocessor.dto.BatchItemResult;
import io.github.itsmejavacoder.txprocessor.dto.BatchSubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.SubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.TransactionRequest;
import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Input adapter for batch and file ingestion. Parsing and bean validation happen per
 * record so one bad record is reported without aborting the rest of the batch.
 */
@Service
public class BatchIngestionService {
    private static final Logger log = LoggerFactory.getLogger(BatchIngestionService.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final TransactionProcessorService processor;
    private final int maxBatchSize;

    public BatchIngestionService(ObjectMapper objectMapper,
                                 Validator validator,
                                 TransactionProcessorService processor,
                                 @Value("${processor.max-batch-size:1000}") int maxBatchSize) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.processor = processor;
        this.maxBatchSize = maxBatchSize;
    }

    public BatchSubmissionResponse ingest(JsonNode payload) {
        JsonNode records = payload;
        if (payload != null && payload.isObject()) {
            records = payload.get("transactions");
        }
        if (records == null || !records.isArray()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Request body must be a JSON array of transactions, or an object with a 'transactions' array");
        }
        if (records.size() > maxBatchSize) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE,
                    "Batch contains " + records.size() + " records; the maximum is " + maxBatchSize);
        }

        List<BatchItemResult> results = new ArrayList<>(records.size());
        int accepted = 0;
        int duplicates = 0;
        int rejected = 0;

        for (int index = 0; index < records.size(); index++) {
            BatchItemResult result = ingestOne(index, records.get(index));
            results.add(result);
            switch (result.outcome()) {
                case "ACCEPTED" -> accepted++;
                case "DUPLICATE" -> duplicates++;
                default -> rejected++;
            }
        }

        log.info("batch.ingested total={} accepted={} duplicates={} rejected={}",
                records.size(), accepted, duplicates, rejected);
        return new BatchSubmissionResponse(records.size(), accepted, duplicates, rejected, results);
    }

    public BatchSubmissionResponse ingestFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A non-empty JSON file is required");
        }
        try (InputStream input = file.getInputStream()) {
            return ingest(objectMapper.readTree(input));
        } catch (JsonProcessingException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Uploaded file is not valid JSON: " + rootMessage(ex));
        } catch (IOException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Could not read the uploaded file");
        }
    }

    private BatchItemResult ingestOne(int index, JsonNode node) {
        if (node == null || !node.isObject()) {
            return new BatchItemResult(index, null, "REJECTED", "Record must be a JSON object");
        }

        TransactionRequest request;
        try {
            request = objectMapper.treeToValue(node, TransactionRequest.class);
        } catch (JsonProcessingException ex) {
            return new BatchItemResult(index, textOrNull(node, "transactionId"), "REJECTED",
                    "Malformed record: " + rootMessage(ex));
        }

        Set<ConstraintViolation<TransactionRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            return new BatchItemResult(index, request.transactionId(), "REJECTED", formatViolations(violations));
        }

        try {
            SubmissionResponse response = processor.submit(request);
            String outcome = response.status() == TransactionStatus.DUPLICATE ? "DUPLICATE" : "ACCEPTED";
            return new BatchItemResult(index, request.transactionId(), outcome, response.message());
        } catch (ResponseStatusException ex) {
            String reason = ex.getReason() == null ? "Record rejected" : ex.getReason();
            return new BatchItemResult(index, request.transactionId(), "REJECTED", reason);
        }
    }

    private String formatViolations(Set<ConstraintViolation<TransactionRequest>> violations) {
        return violations.stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.joining("; "));
    }

    private String textOrNull(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private String rootMessage(JsonProcessingException ex) {
        Throwable cause = ex.getCause();
        if (cause != null && cause.getMessage() != null) {
            String message = cause.getMessage();
            int lineBreak = message.indexOf('\n');
            return lineBreak > 0 ? message.substring(0, lineBreak) : message;
        }
        return ex.getOriginalMessage();
    }
}
