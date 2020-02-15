package com.codenotfound.kafka.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Component;

import com.codenotfound.kafka.kafka.KafkaTopic;

@Component
public class TopicOfIncomingEdigas implements KafkaTopic {

    public static final String PARTITION_SIZE = "5";

    public static final int PARTITION_SIZE_VALUE = Integer.parseInt(PARTITION_SIZE);

    public static final String NAME = "msg-edigas-in";

    public NewTopic buildTopic() {
        return new NewTopic(getName(), getPartitionSize(), (short) 1);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getPartitionSize() {
        return PARTITION_SIZE_VALUE;
    }

    @Override
    public int mapToPartition(String key) {
        return key.hashCode() % PARTITION_SIZE_VALUE;
    }
}
