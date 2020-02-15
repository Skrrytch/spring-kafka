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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.codenotfound.kafka.kafka.EdigasInTopic;
import com.codenotfound.kafka.producer.Sender;

@RestController
public class MessageRestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRestController.class);

    @Autowired
    private Sender sender;

    @Autowired
    private EdigasInTopic topic;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @PostMapping("/send/{id}")
    public String sendMessage(@RequestBody String message, @PathVariable String id) {
        LOGGER.info("Sending message {} with id {}...", message, id);
        sender.send(topic, message, id);
        return "done";
    }

    @PostMapping("/sendBulk")
    public String sendBulkMessages(@RequestBody String messagePrefix) {
        LOGGER.info("Sending 20 bulk messages of id 0...4 and prefix {}");
        for (int i = 0; i < 20; i++) {
            String id = String.valueOf(i % 5);
            sender.send(topic, messagePrefix + "#" + id, id);
        }
        return "done";
    }

    public class TopicStatus {

        String topicName;

        private List<PartitionStatus> partitions;

        public TopicStatus(String topicName, List<PartitionStatus> partitions) {
            this.topicName = topicName;
            this.partitions = partitions;
        }

        public String getTopicName() {
            return topicName;
        }

        public List<PartitionStatus> getPartitions() {
            return partitions;
        }
    }

    public class PartitionStatus implements Comparable {

        private int partitionNumber;

        private String leader;

        public PartitionStatus(PartitionInfo partitionInfo) {
            this.partitionNumber = partitionInfo.partition();
            this.leader = partitionInfo.leader().host() + ":" + partitionInfo.leader().port();
        }

        public int getPartitionNumber() {
            return partitionNumber;
        }

        public String getLeader() {
            return leader;
        }

        @Override
        public int compareTo(Object o) {
            if (!(o instanceof PartitionStatus)) {
                return 1;
            }
            PartitionStatus other = (PartitionStatus) o;
            return Integer.compare(partitionNumber, other.partitionNumber);
        }
    }

    public class KafkaStatus {

        private List<TopicStatus> topics;

        public KafkaStatus(List<TopicStatus> topics) {
            this.topics = topics;
        }

        public List<TopicStatus> getTopics() {
            return topics;
        }
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
