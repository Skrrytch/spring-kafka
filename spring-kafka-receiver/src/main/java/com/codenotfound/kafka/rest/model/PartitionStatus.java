package com.codenotfound.kafka.rest.model;

import org.apache.kafka.common.TopicPartition;

public class PartitionStatus implements Comparable {

    private int partitionNumber;

    public PartitionStatus(TopicPartition partition) {
        this.partitionNumber = partition.partition();
    }

    public int getPartitionNumber() {
        return partitionNumber;
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
