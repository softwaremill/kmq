package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaClients {
    private final KmqConfig kmqConfig;

    public KafkaClients(KmqConfig kmqConfig) {
        this.kmqConfig = kmqConfig;
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySerializer,
                                                     Class<? extends Serializer<V>> valueSerializer) {
        return createProducer(keySerializer, valueSerializer, null);
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySerializer,
                                                     Class<? extends Serializer<V>> valueSerializer,
                                                     Class<? extends KeyValuePartitioner<K, V>> keyValuePartitioner) {
        Map<String, Object> props = (Map)kmqConfig.getProducerProps();
        if (keySerializer != null) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        }
        if (valueSerializer != null) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        }
        if (keyValuePartitioner != null) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, keyValuePartitioner);
        }

        return new KafkaProducer<>(props);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId,
                                                     Class<? extends Deserializer<K>> keyDeserializer,
                                                     Class<? extends Deserializer<V>> valueDeserializer) {
        Map<String, Object> props = (Map)kmqConfig.getConsumerProps();
        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (keyDeserializer != null) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        }
        if (valueDeserializer != null) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        }

        return new KafkaConsumer<>(props);
    }
}
