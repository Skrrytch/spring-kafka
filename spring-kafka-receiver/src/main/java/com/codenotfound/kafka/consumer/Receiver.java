package com.codenotfound.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.codenotfound.kafka.kafka.KafkaDefinition;

@Service
public class Receiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private CountDownLatch latch = new CountDownLatch(2);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(
            groupId = KafkaDefinition.GROUP_ID,
            id = TopicOfIncomingEdigas.NAME,
            topics = TopicOfIncomingEdigas.NAME,
            concurrency = TopicOfIncomingEdigas.PARTITION_SIZE,
            autoStartup = "true")
    public void receiveBar(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("\"{}\": received from partition {}", message, partition);
        int waitLoop = 10;
        for (int count = 1; count < waitLoop; count++) {
            try {
                Thread.sleep(1000);
                LOGGER.info("\"{}\": {} of {}", message, count, waitLoop);
            } catch (InterruptedException e) {
            }
        }
        LOGGER.info("\"{}\": done", message);
        latch.countDown();
    }
}
