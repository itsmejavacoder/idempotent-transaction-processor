package io.github.itsmejavacoder.txprocessor.controller;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.itsmejavacoder.txprocessor.dto.AccountView;
import io.github.itsmejavacoder.txprocessor.dto.BatchSubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.LedgerEntryView;
import io.github.itsmejavacoder.txprocessor.dto.ProcessingSummary;
import io.github.itsmejavacoder.txprocessor.dto.SequenceReport;
import io.github.itsmejavacoder.txprocessor.dto.SubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.TransactionEventView;
import io.github.itsmejavacoder.txprocessor.dto.TransactionRequest;
import io.github.itsmejavacoder.txprocessor.dto.TransactionView;
import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;
import io.github.itsmejavacoder.txprocessor.service.BatchIngestionService;
import io.github.itsmejavacoder.txprocessor.service.TransactionProcessorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@Tag(name = "Transactions", description = "Ingestion, processing, account and ledger queries")
public class TransactionController {
    private final TransactionProcessorService service;
    private final BatchIngestionService batchIngestion;

    public TransactionController(TransactionProcessorService service, BatchIngestionService batchIngestion) {
        this.service = service;
        this.batchIngestion = batchIngestion;
    }

    @Operation(summary = "Submit one transaction", description = "Validates the request, applies duplicate checks and persists it as RECEIVED. Returns 202, or 200 when the transaction is a duplicate.")
    @PostMapping("/transactions")
    public ResponseEntity<SubmissionResponse> submit(@Valid @RequestBody TransactionRequest request) {
        SubmissionResponse response = service.submit(request);
        HttpStatus status = response.status() == TransactionStatus.DUPLICATE
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(response);
    }

    @Operation(summary = "Submit a JSON batch", description = "Accepts a JSON array or an object with a transactions array. Each element is validated and reported independently.")
    @PostMapping("/transactions/batch")
    public BatchSubmissionResponse submitBatch(@RequestBody JsonNode payload) {
        return batchIngestion.ingest(payload);
    }

    @Operation(summary = "Upload a JSON file", description = "Multipart upload of a JSON transaction array; each record gets an independent outcome.")
    @PostMapping(value = "/transactions/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public BatchSubmissionResponse upload(@RequestParam("file") MultipartFile file) {
        return batchIngestion.ingestFile(file);
    }

    @Operation(summary = "Get transaction state", description = "Current status, attempts, last error and applied sequence.")
    @GetMapping("/transactions/{transactionId}")
    public TransactionView getTransaction(@PathVariable String transactionId) {
        return service.getTransaction(transactionId);
    }

    @Operation(summary = "Get transaction event history", description = "Ordered status transitions, including failure reasons.")
    @GetMapping("/transactions/{transactionId}/events")
    public List<TransactionEventView> getEvents(@PathVariable String transactionId) {
        return service.getEvents(transactionId);
    }

    @Operation(summary = "Run one processing pass", description = "Worker-equivalent manual trigger. Returns the number of records touched.")
    @PostMapping("/transactions/process")
    public Map<String, Object> processNow() {
        int touched = service.processPendingTransactions();
        return Map.of("message", "Processing pass completed", "recordsTouched", touched);
    }

    @Operation(summary = "Get processing summary", description = "Counts by status plus duplicate and ledger totals.")
    @GetMapping("/transactions/summary")
    public ProcessingSummary summary() {
        return service.getSummary();
    }

    @Operation(summary = "Get sequence gap report", description = "Stream position, highest observed sequence, missing sequences and blocked records.")
    @GetMapping("/processing/sequence-report")
    public SequenceReport sequenceReport() {
        return service.getSequenceReport();
    }

    @Operation(summary = "Get account balance", description = "Balance, currency and the last applied sequence for an account.")
    @GetMapping("/accounts/{accountId}")
    public AccountView account(@PathVariable String accountId) {
        return service.getAccount(accountId);
    }

    @Operation(summary = "Get ledger entries", description = "Immutable applied effects in insertion order.")
    @GetMapping("/ledger")
    public List<LedgerEntryView> ledger() {
        return service.getLedger();
    }
}
