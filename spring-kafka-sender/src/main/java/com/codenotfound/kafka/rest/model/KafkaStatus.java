package com.codenotfound.kafka.rest.model;

import java.util.List;

public class KafkaStatus {

    private List<TopicStatus> topics;

    public KafkaStatus(List<TopicStatus> topics) {
        this.topics = topics;
    }

    public List<TopicStatus> getTopics() {
        return topics;
    }
}
