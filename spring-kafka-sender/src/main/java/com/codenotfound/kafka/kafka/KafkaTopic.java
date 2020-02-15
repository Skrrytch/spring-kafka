package com.codenotfound.kafka.kafka;

public interface KafkaTopic {

    String getName();

    int getPartitionSize();

    int mapToPartition(String key);
}
