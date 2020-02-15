package com.codenotfound.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaAdmin;

import com.codenotfound.kafka.KafkaDefinition;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Bean
    public Map<String, Object> adminConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return configs;
    }

    @Bean
    public KafkaAdmin kafkaAdmin(Map<String, Object> adminConfigs) {
        return new KafkaAdmin(adminConfigs);
    }

    @Bean
    public NewTopic incomingEdigasTopic() {
        return new NewTopic(KafkaDefinition.TOPIC_MSG_EDIGAS_INCOMING, 10, (short) 1);
    }

}
