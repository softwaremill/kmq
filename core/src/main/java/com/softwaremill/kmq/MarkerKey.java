package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class MarkerKey {
    private final int partition;
    private final long messageOffset;

    public MarkerKey(int partition, long messageOffset) {
        this.partition = partition;
        this.messageOffset = messageOffset;
    }

    public int getPartition() {
        return partition;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    public byte[] serialize() {
        return ByteBuffer.allocate(4+8)
                .putInt(partition)
                .putLong(messageOffset)
                .array();
    }

    @Override
    public String toString() {
        return "MarkerKey{" +
                "partition=" + partition +
                ", messageOffset=" + messageOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MarkerKey markerKey = (MarkerKey) o;
        return partition == markerKey.partition &&
                messageOffset == markerKey.messageOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, messageOffset);
    }

    public static class MarkerKeySerializer implements Serializer<MarkerKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public byte[] serialize(String topic, MarkerKey data) {
            return data.serialize();
        }

        @Override
        public void close() {}
    }

    public static class MarkerKeyDeserializer implements Deserializer<MarkerKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public MarkerKey deserialize(String topic, byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            return new MarkerKey(bb.getInt(), bb.getLong());
        }

        @Override
        public void close() {}
    }

    public static class MarkerKeySerde implements Serde<MarkerKey> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}

        @Override
        public Serializer<MarkerKey> serializer() {
            return new MarkerKeySerializer();
        }

        @Override
        public Deserializer<MarkerKey> deserializer() {
            return new MarkerKeyDeserializer();
        }
    }

    public static MarkerKey fromRecord(ConsumerRecord r) {
        return new MarkerKey(r.partition(), r.offset());
    }
}
