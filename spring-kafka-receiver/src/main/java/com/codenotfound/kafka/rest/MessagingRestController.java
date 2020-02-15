package com.codenotfound.kafka.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.codenotfound.kafka.consumer.TopicOfIncomingEdigas;
import com.codenotfound.kafka.rest.model.KafkaStatus;
import com.codenotfound.kafka.rest.model.PartitionStatus;
import com.codenotfound.kafka.rest.model.TopicStatus;

@RestController
public class MessagingRestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingRestController.class);

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @PostMapping("/consumers/pause")
    public void pauseTopic() {
        String topic = TopicOfIncomingEdigas.NAME;
        LOGGER.info("Pausing all consumers for topic {} ...", topic);
        MessageListenerContainer container = registry.getListenerContainer(TopicOfIncomingEdigas.NAME);
        if (container != null) {
            if (container.isContainerPaused()) {
                LOGGER.info("Pause already active for {}", topic);
            } else if (container.isPauseRequested()) {
                LOGGER.info("Pause already requested for {}", topic);
            } else {
                container.pause();
                LOGGER.info("Pause for {} requested", topic);
            }
        } else {
            LOGGER.error("No container found for  topic {}", topic);
        }
    }

    @PostMapping("/consumers/resume")
    public void resumeTopic() {
        String topic = TopicOfIncomingEdigas.NAME;
        LOGGER.info("Resuming all consumers for topic {} ...", topic);
        MessageListenerContainer container = registry.getListenerContainer(topic);
        if (container != null) {
            if (container.isContainerPaused() || container.isPauseRequested()) {
                container.resume();
                LOGGER.info("Resume for {} requested!", topic);
            } else {
                LOGGER.info("Not paused or requested for {}", topic);
            }
        } else {
            LOGGER.info("No container found for topic {}", topic);
        }
    }

    @GetMapping("/status")
    public KafkaStatus getStatus() {
        List<TopicStatus> topics = new ArrayList<>();

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            Map<String, List<PartitionInfo>> map = consumer.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : map.entrySet()) {
                String topicName = entry.getKey();
                MessageListenerContainer container = registry.getListenerContainer(TopicOfIncomingEdigas.NAME);
                Collection<TopicPartition> assignedPartitions = container.getAssignedPartitions();
                List<PartitionStatus> partitions = new ArrayList<>();
                for (TopicPartition partition : assignedPartitions) {
                    partitions.add(new PartitionStatus(partition));
                }
                Collections.sort(partitions);
                topics.add(new TopicStatus(topicName, partitions, container));
            }
        }

        return new KafkaStatus(topics);
    }
}
