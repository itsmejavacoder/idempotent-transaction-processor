package io.github.itsmejavacoder.txprocessor;

import io.github.itsmejavacoder.txprocessor.dto.BatchSubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.SequenceReport;
import io.github.itsmejavacoder.txprocessor.dto.SubmissionResponse;
import io.github.itsmejavacoder.txprocessor.dto.TransactionEventView;
import io.github.itsmejavacoder.txprocessor.dto.TransactionRequest;
import io.github.itsmejavacoder.txprocessor.dto.TransactionView;
import io.github.itsmejavacoder.txprocessor.model.TransactionStatus;
import io.github.itsmejavacoder.txprocessor.model.TransactionType;
import io.github.itsmejavacoder.txprocessor.service.TransactionProcessorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TransactionProcessingIntegrationTest {
    @Autowired TransactionProcessorService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mockMvc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("DELETE FROM transaction_events");
        jdbc.update("DELETE FROM duplicate_requests");
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM accounts");
        jdbc.update("UPDATE processing_stream SET last_sequence = 0");
    }

    @Test
    void duplicateTransactionIdCreatesOneLedgerEntryAndIsCounted() {
        TransactionRequest request = request("TX-1", "REQ-1", "ACC-1",
                TransactionType.CREDIT, "100.00", 1, 0);

        assertThat(service.submit(request).status()).isEqualTo(TransactionStatus.RECEIVED);
        assertThat(service.submit(request).status()).isEqualTo(TransactionStatus.DUPLICATE);
        service.processPendingTransactions();

        assertThat(service.getTransaction("TX-1").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-1").balance()).isEqualByComparingTo("100.00");
        assertThat(countLedger("TX-1")).isEqualTo(1);
        assertThat(service.getSummary().duplicateRequests()).isEqualTo(1);
        assertThat(service.getSummary().transactionCountsByStatus().get("DUPLICATE")).isEqualTo(1);
    }

    @Test
    void duplicateRequestIdForAnotherTransactionIsRejected() {
        service.submit(request("TX-A", "REQ-A", "ACC-1", TransactionType.CREDIT, "10.00", 1, 0));

        assertThatThrownBy(() -> service.submit(
                request("TX-B", "REQ-A", "ACC-1", TransactionType.CREDIT, "20.00", 2, 0)))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("requestId already belongs to transaction TX-A");

        assertThat(countTransactions()).isEqualTo(1);
    }

    @Test
    void sameTransactionIdWithDifferentPayloadReturnsConflict() {
        service.submit(request("TX-CONFLICT", "REQ-1", "ACC-5",
                TransactionType.CREDIT, "100.00", 1, 0));

        assertThatThrownBy(() -> service.submit(
                request("TX-CONFLICT", "REQ-2", "ACC-5", TransactionType.CREDIT, "200.00", 1, 0)))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("different payload");
    }

    @Test
    void duplicateSequenceNumberForAnotherTransactionIsRejected() {
        service.submit(request("TX-S1", "REQ-S1", "ACC-1", TransactionType.CREDIT, "10.00", 1, 0));

        assertThatThrownBy(() -> service.submit(
                request("TX-S2", "REQ-S2", "ACC-2", TransactionType.CREDIT, "20.00", 1, 0)))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("already assigned to transaction TX-S1");
    }

    @Test
    void outOfOrderRecordWaitsForGlobalPredecessorOnAnotherAccount() {
        service.submit(request("TX-2", "REQ-2", "ACC-A", TransactionType.CREDIT, "40.00", 2, 0));
        service.processPendingTransactions();
        assertThat(service.getTransaction("TX-2").status()).isEqualTo(TransactionStatus.PENDING_SEQUENCE);

        // Sequence 1 belongs to a different account, which proves the stream is global.
        service.submit(request("TX-1", "REQ-1", "ACC-B", TransactionType.CREDIT, "100.00", 1, 0));
        service.processPendingTransactions();

        assertThat(service.getTransaction("TX-1").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getTransaction("TX-2").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-A").balance()).isEqualByComparingTo("40.00");
        assertThat(service.getAccount("ACC-B").balance()).isEqualByComparingTo("100.00");
    }

    @Test
    void missingSequenceIsReportedWhileLaterRecordWaits() {
        service.submit(request("TX-1", "REQ-1", "ACC-1", TransactionType.CREDIT, "100.00", 1, 0));
        service.submit(request("TX-3", "REQ-3", "ACC-1", TransactionType.CREDIT, "25.00", 3, 0));
        service.processPendingTransactions();

        SequenceReport report = service.getSequenceReport();
        assertThat(report.lastProcessedSequence()).isEqualTo(1);
        assertThat(report.highestObservedSequence()).isEqualTo(3);
        assertThat(report.missingSequences()).containsExactly(2L);
        assertThat(report.pendingTransactions())
                .extracting(pending -> pending.transactionId())
                .containsExactly("TX-3");
        assertThat(service.getTransaction("TX-3").status()).isEqualTo(TransactionStatus.PENDING_SEQUENCE);
    }

    @Test
    void transientFailureRetriesWithoutDuplicateEffect() {
        service.submit(request("TX-RETRY", "REQ-RETRY", "ACC-3",
                TransactionType.CREDIT, "50.00", 1, 1));

        service.processPendingTransactions();
        assertThat(service.getTransaction("TX-RETRY").status()).isEqualTo(TransactionStatus.RETRY_PENDING);
        assertThat(service.getAccount("ACC-3").balance()).isEqualByComparingTo("0.00");

        service.processPendingTransactions();
        TransactionView result = service.getTransaction("TX-RETRY");
        assertThat(result.status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(result.attempts()).isEqualTo(2);
        assertThat(service.getAccount("ACC-3").balance()).isEqualByComparingTo("50.00");
        assertThat(countLedger("TX-RETRY")).isEqualTo(1);
    }

    @Test
    void retryExhaustionFailsRecordAndUnblocksTheStream() {
        service.submit(request("TX-FAIL", "REQ-FAIL", "ACC-4",
                TransactionType.CREDIT, "500.00", 1, 99));

        for (int i = 0; i < 4; i++) {
            service.processPendingTransactions();
        }

        assertThat(service.getTransaction("TX-FAIL").status()).isEqualTo(TransactionStatus.FAILED);
        assertThat(service.getAccount("ACC-4").balance()).isEqualByComparingTo("0.00");
        assertThat(countLedger("TX-FAIL")).isZero();

        service.submit(request("TX-AFTER", "REQ-AFTER", "ACC-4",
                TransactionType.CREDIT, "50.00", 2, 0));
        service.processPendingTransactions();
        assertThat(service.getTransaction("TX-AFTER").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-4").balance()).isEqualByComparingTo("50.00");
    }

    @Test
    void insufficientFundsFailsRecordAndUnblocksTheStream() {
        service.submit(request("TX-DEBIT", "REQ-DEBIT", "ACC-6",
                TransactionType.DEBIT, "10.00", 1, 0));
        service.processPendingTransactions();
        assertThat(service.getTransaction("TX-DEBIT").status()).isEqualTo(TransactionStatus.FAILED);

        service.submit(request("TX-CREDIT", "REQ-CREDIT", "ACC-6",
                TransactionType.CREDIT, "50.00", 2, 0));
        service.processPendingTransactions();

        assertThat(service.getTransaction("TX-CREDIT").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-6").balance()).isEqualByComparingTo("50.00");
    }

    @Test
    void currencyMismatchIsRejectedOnceTheAccountExists() {
        service.submit(request("TX-USD", "REQ-USD", "ACC-7",
                TransactionType.CREDIT, "100.00", 1, 0));
        service.processPendingTransactions();

        assertThatThrownBy(() -> service.submit(request("TX-EUR", "REQ-EUR", "ACC-7",
                TransactionType.CREDIT, "50.00", 2, 0, "EUR")))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("uses currency USD");

        assertThat(countTransactions()).isEqualTo(1);
    }

    @Test
    void batchIsolatesInvalidAndDuplicateRecordsAndProcessesTheRest() throws Exception {
        String json = """
                [
                  {"transactionId":"TX-B1","requestId":"REQ-B1","accountId":"ACC-B","transactionType":"CREDIT","amount":100.00,"currency":"USD","sequenceNumber":1,"failuresBeforeSuccess":0},
                  {"transactionId":"TX-B2","requestId":"REQ-B2","accountId":"ACC-B","transactionType":"DEBIT","amount":40.00,"currency":"USD","sequenceNumber":2,"failuresBeforeSuccess":0},
                  {"transactionId":"TX-B3","requestId":"REQ-B3","accountId":"ACC-B","transactionType":"CREDIT","amount":25.00,"currency":"USD","sequenceNumber":3,"failuresBeforeSuccess":0},
                  {"transactionId":"TX-BAD","requestId":"REQ-BAD","accountId":"ACC-B","transactionType":"CREDIT","amount":-10,"currency":"USD","sequenceNumber":9},
                  {"transactionId":"TX-B1","requestId":"REQ-B1-DUP","accountId":"ACC-B","transactionType":"CREDIT","amount":100.00,"currency":"USD","sequenceNumber":1}
                ]
                """;

        mockMvc.perform(post("/api/transactions/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(5))
                .andExpect(jsonPath("$.accepted").value(3))
                .andExpect(jsonPath("$.duplicates").value(1))
                .andExpect(jsonPath("$.rejected").value(1))
                .andExpect(jsonPath("$.results[3].outcome").value("REJECTED"))
                .andExpect(jsonPath("$.results[3].message").value(
                        org.hamcrest.Matchers.containsString("amount must be greater than zero")))
                .andExpect(jsonPath("$.results[4].outcome").value("DUPLICATE"));

        service.processPendingTransactions();

        assertThat(service.getTransaction("TX-B1").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getTransaction("TX-B2").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getTransaction("TX-B3").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-B").balance()).isEqualByComparingTo("85.00");
        assertThat(countLedger("TX-B1")).isEqualTo(1);
    }

    @Test
    void malformedBatchRecordIsIsolated() throws Exception {
        String json = """
                [
                  {"transactionId":"TX-M1","requestId":"REQ-M1","accountId":"ACC-M","transactionType":"TRANSFER","amount":10,"currency":"USD","sequenceNumber":1},
                  {"transactionId":"TX-M2","requestId":"REQ-M2","accountId":"ACC-M","transactionType":"CREDIT","amount":10,"currency":"USD","sequenceNumber":2}
                ]
                """;

        mockMvc.perform(post("/api/transactions/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(1))
                .andExpect(jsonPath("$.rejected").value(1))
                .andExpect(jsonPath("$.results[0].outcome").value("REJECTED"))
                .andExpect(jsonPath("$.results[0].message").value(
                        org.hamcrest.Matchers.containsString("Malformed record")));

        assertThat(countTransactions()).isEqualTo(1);
    }

    @Test
    void uploadedFileIsIngestedPerRecord() throws Exception {
        String json = """
                [
                  {"transactionId":"TX-U1","requestId":"REQ-U1","accountId":"ACC-U","transactionType":"CREDIT","amount":75.00,"currency":"INR","sequenceNumber":1,"failuresBeforeSuccess":0},
                  {"transactionId":"TX-U2","requestId":"REQ-U2","accountId":"ACC-U","transactionType":"CREDIT","amount":0,"currency":"INR","sequenceNumber":2}
                ]
                """;
        MockMultipartFile file = new MockMultipartFile(
                "file", "transactions.json", "application/json", json.getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(multipart("/api/transactions/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.accepted").value(1))
                .andExpect(jsonPath("$.rejected").value(1));

        service.processPendingTransactions();
        assertThat(service.getTransaction("TX-U1").status()).isEqualTo(TransactionStatus.PROCESSED);
        assertThat(service.getAccount("ACC-U").balance()).isEqualByComparingTo("75.00");
    }

    @Test
    void invalidApiRequestReturnsConsistentErrorBody() throws Exception {
        String json = """
                {
                  "transactionId":"TX-INVALID",
                  "requestId":"REQ-INVALID",
                  "accountId":"ACC-INVALID",
                  "transactionType":"CREDIT",
                  "amount":-10,
                  "currency":"USD",
                  "sequenceNumber":1,
                  "failuresBeforeSuccess":0
                }
                """;

        mockMvc.perform(post("/api/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value(
                        org.hamcrest.Matchers.containsString("amount must be greater than zero")));

        assertThat(countTransactions()).isZero();
    }

    @Test
    void unknownPathReturnsConsistent404() throws Exception {
        mockMvc.perform(get("/api/does-not-exist"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("Resource not found"));
    }

    @Test
    void lifecycleEventsAreRecorded() {        service.submit(request("TX-EVENT", "REQ-EVENT", "ACC-E",
                TransactionType.CREDIT, "10.00", 1, 0));
        service.processPendingTransactions();

        List<TransactionEventView> events = service.getEvents("TX-EVENT");
        assertThat(events).extracting(TransactionEventView::status)
                .containsExactly("RECEIVED", "PROCESSING", "PROCESSED");
    }

    @Test
    void concurrentDuplicateSubmissionsCreateOneLedgerEntry() throws Exception {
        TransactionRequest request = request("TX-RACE", "REQ-RACE", "ACC-R",
                TransactionType.CREDIT, "100.00", 1, 0);
        int threads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<SubmissionResponse>> futures = java.util.stream.IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        ready.countDown();
                        start.await();
                        return service.submit(request);
                    }))
                    .toList();

            ready.await();
            start.countDown();

            long duplicates = 0;
            for (Future<SubmissionResponse> future : futures) {
                if (future.get().status() == TransactionStatus.DUPLICATE) {
                    duplicates++;
                }
            }
            assertThat(duplicates).isEqualTo(threads - 1);
        } finally {
            pool.shutdownNow();
        }

        service.processPendingTransactions();
        assertThat(service.getAccount("ACC-R").balance()).isEqualByComparingTo("100.00");
        assertThat(countLedger("TX-RACE")).isEqualTo(1);
    }

    @Test
    void openApiDocumentIsExposedWithEveryApiPath() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title")
                        .value("Idempotent Background Transaction Processing Service"))
                .andExpect(jsonPath("$.paths['/api/transactions']").exists())
                .andExpect(jsonPath("$.paths['/api/transactions/batch']").exists())
                .andExpect(jsonPath("$.paths['/api/transactions/upload']").exists())
                .andExpect(jsonPath("$.paths['/api/transactions/process']").exists())
                .andExpect(jsonPath("$.paths['/api/transactions/summary']").exists())
                .andExpect(jsonPath("$.paths['/api/processing/sequence-report']").exists())
                .andExpect(jsonPath("$.paths['/api/ledger']").exists());
    }

    private TransactionRequest request(String txId, String requestId, String accountId,
                                       TransactionType type, String amount, long sequence, int failures) {
        return request(txId, requestId, accountId, type, amount, sequence, failures, "USD");
    }

    private TransactionRequest request(String txId, String requestId, String accountId,
                                       TransactionType type, String amount, long sequence,
                                       int failures, String currency) {
        return new TransactionRequest(txId, requestId, accountId, type,
                new BigDecimal(amount), currency, sequence, failures);
    }

    private int countLedger(String transactionId) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM ledger_entries WHERE transaction_id=?", Integer.class, transactionId);
        return count == null ? 0 : count;
    }

    private int countTransactions() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM transactions", Integer.class);
        return count == null ? 0 : count;
    }
}
