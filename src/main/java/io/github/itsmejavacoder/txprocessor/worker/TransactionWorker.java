package io.github.itsmejavacoder.txprocessor.worker;

import io.github.itsmejavacoder.txprocessor.service.TransactionProcessorService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "processor.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class TransactionWorker {
    private final TransactionProcessorService service;

    public TransactionWorker(TransactionProcessorService service) {
        this.service = service;
    }

    @Scheduled(fixedDelayString = "${processor.poll-interval-ms:1000}")
    public void processPending() {
        service.processPendingTransactions();
    }
}
