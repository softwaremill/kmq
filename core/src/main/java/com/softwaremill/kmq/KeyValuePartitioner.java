package com.softwaremill.kmq;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Assigns partitions basing on the key and value.
 */
public abstract class KeyValuePartitioner<K, V> implements Partitioner {

    public abstract int partition(K key, V value);

    @Override
    @SuppressWarnings("unchecked")
    public final int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partition((K)key, (V)value);
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
