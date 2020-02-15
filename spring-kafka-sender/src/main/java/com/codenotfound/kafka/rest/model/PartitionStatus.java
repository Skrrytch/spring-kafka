package com.codenotfound.kafka.rest.model;

import org.apache.kafka.common.PartitionInfo;

import com.codenotfound.kafka.rest.MessagingRestController;

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
