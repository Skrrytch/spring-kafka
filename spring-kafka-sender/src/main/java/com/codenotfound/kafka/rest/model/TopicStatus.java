package com.codenotfound.kafka.rest.model;

import java.util.List;

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
