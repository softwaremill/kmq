package com.softwaremill.kmq;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Assigns partitions basing on the partition contained in the key, which must be a `MarkerKey`.
 */
public class ParititionFromMarkerKey implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return ((MarkerKey) key).getPartition();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
