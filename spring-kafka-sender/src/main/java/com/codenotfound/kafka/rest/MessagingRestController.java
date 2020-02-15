package com.codenotfound.kafka.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.codenotfound.kafka.kafka.EdigasInTopic;
import com.codenotfound.kafka.producer.Sender;
import com.codenotfound.kafka.rest.model.KafkaStatus;
import com.codenotfound.kafka.rest.model.PartitionStatus;
import com.codenotfound.kafka.rest.model.TopicStatus;

@RestController
public class MessagingRestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingRestController.class);

    @Autowired
    private Sender sender;

    @Autowired
    private EdigasInTopic topic;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @PostMapping("/send/{id}/{message}")
    public String sendMessage(@PathVariable String id, @PathVariable String message) {
        LOGGER.info("Sending message {} with id {}...", message, id);
        sender.send(topic, message, id);
        return "done";
    }

    @PostMapping("/sendBulk/{messagePrefix}")
    public String sendBulkMessages(@PathVariable String messagePrefix) {
        int amount = 10;
        LOGGER.info("Sending {} bulk messages of id 0...4 and prefix {}", amount, messagePrefix);
        for (int i = 0; i < amount; i++) {
            String id = String.valueOf(i % 5);
            sender.send(topic, messagePrefix + "#" + id + "." + ((i / 5) + 1), id);
        }
        return "done";
    }

    @GetMapping("/status")
    public KafkaStatus getStatus() {
        List<TopicStatus> topics = new ArrayList<>();

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            Map<String, List<PartitionInfo>> map = consumer.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : map.entrySet()) {
                List<PartitionStatus> partitions = new ArrayList<>();
                for (PartitionInfo partitionInfo : entry.getValue()) {
                    partitions.add(new PartitionStatus(partitionInfo));
                }
                Collections.sort(partitions);
                topics.add(new TopicStatus(entry.getKey(), partitions));
            }
        }

        return new KafkaStatus(topics);
    }
}
