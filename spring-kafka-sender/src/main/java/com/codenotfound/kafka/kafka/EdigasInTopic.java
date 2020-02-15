package com.codenotfound.kafka.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Component;

@Component
public class EdigasInTopic implements KafkaTopic {

    private static final int PARTITION_SIZE = 10;

    public NewTopic buildTopic() {
        return new NewTopic(getName(), getPartitionSize(), (short) 1);
    }

    @Override
    public String getName() {
        return "msg-edigas-in";
    }

    @Override
    public int getPartitionSize() {
        return PARTITION_SIZE;
    }

    @Override
    public int mapToPartition(String key) {
        return key.hashCode() % PARTITION_SIZE;
    }
}
