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
public class TransactionController {
    private final TransactionProcessorService service;
    private final BatchIngestionService batchIngestion;

    public TransactionController(TransactionProcessorService service, BatchIngestionService batchIngestion) {
        this.service = service;
        this.batchIngestion = batchIngestion;
    }

    @PostMapping("/transactions")
    public ResponseEntity<SubmissionResponse> submit(@Valid @RequestBody TransactionRequest request) {
        SubmissionResponse response = service.submit(request);
        HttpStatus status = response.status() == TransactionStatus.DUPLICATE
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED;
        return ResponseEntity.status(status).body(response);
    }

    @PostMapping("/transactions/batch")
    public BatchSubmissionResponse submitBatch(@RequestBody JsonNode payload) {
        return batchIngestion.ingest(payload);
    }

    @PostMapping(value = "/transactions/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public BatchSubmissionResponse upload(@RequestParam("file") MultipartFile file) {
        return batchIngestion.ingestFile(file);
    }

    @GetMapping("/transactions/{transactionId}")
    public TransactionView getTransaction(@PathVariable String transactionId) {
        return service.getTransaction(transactionId);
    }

    @GetMapping("/transactions/{transactionId}/events")
    public List<TransactionEventView> getEvents(@PathVariable String transactionId) {
        return service.getEvents(transactionId);
    }

    @PostMapping("/transactions/process")
    public Map<String, Object> processNow() {
        int touched = service.processPendingTransactions();
        return Map.of("message", "Processing pass completed", "recordsTouched", touched);
    }

    @GetMapping("/transactions/summary")
    public ProcessingSummary summary() {
        return service.getSummary();
    }

    @GetMapping("/processing/sequence-report")
    public SequenceReport sequenceReport() {
        return service.getSequenceReport();
    }

    @GetMapping("/accounts/{accountId}")
    public AccountView account(@PathVariable String accountId) {
        return service.getAccount(accountId);
    }

    @GetMapping("/ledger")
    public List<LedgerEntryView> ledger() {
        return service.getLedger();
    }
}
