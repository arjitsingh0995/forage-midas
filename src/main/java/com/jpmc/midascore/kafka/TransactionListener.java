package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final BlockingQueue<Transaction> receivedTransactions = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-consumer")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        receivedTransactions.offer(transaction);
    }

    public BlockingQueue<Transaction> getReceivedTransactions() {
        return receivedTransactions;



        
    }
}
