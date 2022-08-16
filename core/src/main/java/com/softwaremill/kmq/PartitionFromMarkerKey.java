package com.softwaremill.kmq;

/**
 * Assigns partitions basing on the partition contained in the key, which must be a `MarkerKey`.
 */
public class PartitionFromMarkerKey extends KeyValuePartitioner<MarkerKey, MarkerValue> {
    @Override
    public int partition(MarkerKey key, MarkerValue value) {
        return key.getPartition();
    }
}
