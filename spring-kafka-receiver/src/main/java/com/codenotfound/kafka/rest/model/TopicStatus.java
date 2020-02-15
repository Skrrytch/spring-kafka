package com.codenotfound.kafka.rest.model;

import java.util.List;

import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

public class TopicStatus {

    private final boolean containerPaused;

    private final boolean pauseRequested;

    String topicName;

    private List<PartitionStatus> partitions;

    public TopicStatus(
            String topicName,
            List<PartitionStatus> partitions,
            MessageListenerContainer container) {
        this.topicName = topicName;
        this.partitions = partitions;
        this.containerPaused = container.isContainerPaused();
        this.pauseRequested = container.isPauseRequested();
    }

    public String getTopicName() {
        return topicName;
    }

    public List<PartitionStatus> getPartitions() {
        return partitions;
    }

    public boolean isContainerPaused() {
        return containerPaused;
    }

    public boolean isPauseRequested() {
        return pauseRequested;
    }
}


