package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaClients {
    private final String bootstrapServer;

    public KafkaClients(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer) {
        return createProducer(keySerializer, valueSerializer, Collections.emptyMap());
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer,
                                                            Map<String, Object> extraConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", keySerializer.getName());
        props.put("value.serializer", valueSerializer.getName());
        for (Map.Entry<String, Object> extraCfgEntry : extraConfig.entrySet()) {
            props.put(extraCfgEntry.getKey(), extraCfgEntry.getValue());
        }

        return new KafkaProducer<>(props);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId,
                                                     Class<? extends Deserializer<K>> keyDeserializer,
                                                     Class<? extends Deserializer<V>> valueDeserializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", keyDeserializer.getName());
        props.put("value.deserializer", valueDeserializer.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        return new KafkaConsumer<>(props);
    }
}
