package com.codenotfound.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.codenotfound.kafka.kafka.KafkaDefinition;

@Configuration
public class ReceiverConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverConfig.class);

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        int maxMessageProcessingInSeconds = 5 * 60;
        int maxMessagePrefetch = 1;

        LOGGER.info("Kafka consumer factory config ...");
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaDefinition.GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Der folgende Prefetch-Wert ist per Default sehr hoch:
        // Das folgt dazu, dass ein Pausieren von Listenern warten muss, bis alle per Poll geholten Nachrichten
        // abgearbeitet sind. Bei langsamer verarbeitung macht ein kleiner Wert hier mehr Sinn.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessagePrefetch);
        // The maximum delay between invocations of poll() when using consumer group management. This places an upper bound on
        // the amount of time that the consumer can be idle before fetching more records. If poll() is not called before
        // expiration of this timeout, then the consumer is considered failed and the group will rebalance in order to reassign
        // the partitions to another member. (Default 30000)
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * maxMessageProcessingInSeconds); // 5 Minutes!
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        LOGGER.info("Kafka listener container factory config ...");
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        return factory;
    }
}
