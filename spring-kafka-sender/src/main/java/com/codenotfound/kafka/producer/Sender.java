package com.codenotfound.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.codenotfound.kafka.kafka.EdigasInTopic;

public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(EdigasInTopic topic, String payload, String partitionKey) {
        int partition = topic.mapToPartition(partitionKey);
        LOGGER.info("Sending payload='{}' to topic='{}' and partition={} ({})", payload, topic, partitionKey, partition);
        kafkaTemplate.send(topic.getName(), partition, "key", payload)
                .addCallback(new ListenableFutureCallback<>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        System.out.println("Sent message=[" + payload + "] with offset=[" + result.getRecordMetadata()
                                .offset() + "]");
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        System.out.println("Unable to send message=["
                                + payload + "] due to : " + ex.getMessage());
                    }
                });
    }

    public void send(String topic, String payload) {
        LOGGER.info("Sending payload='{}' to topic='{}'", payload, topic);
        kafkaTemplate.send(topic, payload)
                .addCallback(new ListenableFutureCallback<>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        System.out.println("Sent message=[" + payload + "] with offset=[" + result.getRecordMetadata()
                                .offset() + "]");
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        System.out.println("Unable to send message=["
                                + payload + "] due to : " + ex.getMessage());
                    }
                });
    }
}
