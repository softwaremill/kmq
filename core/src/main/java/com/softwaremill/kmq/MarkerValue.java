package com.softwaremill.kmq;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class MarkerValue {
    private final boolean start;
    private final long processingTimestamp;

    public MarkerValue(boolean start, long processingTimestamp) {
        this.start = start;
        this.processingTimestamp = processingTimestamp;
    }

    public boolean isStart() {
        return start;
    }

    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    public byte[] serialize() {
        return ByteBuffer.allocate(1 + 8)
                .put((byte) (start ? 1 : 0))
                .putLong(processingTimestamp)
                .array();
    }

    @Override
    public String toString() {
        return "MarkerValue{" +
                "start=" + start +
                ", processingTimestamp=" + processingTimestamp +
                '}';
    }

    public static class MarkerValueSerializer implements Serializer<MarkerValue> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public byte[] serialize(String topic, MarkerValue data) {
            if (data == null) {
                return new byte[0];
            } else {
                return data.serialize();
            }
        }

        @Override
        public void close() {}
    }

    public static class MarkerValueDeserializer implements Deserializer<MarkerValue> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public MarkerValue deserialize(String topic, byte[] data) {
            if (data.length == 0) {
                return null;
            } else {
                ByteBuffer bb = ByteBuffer.wrap(data);
                return new MarkerValue(bb.get() == 1, bb.getLong());
            }
        }

        @Override
        public void close() {}
    }

    public static class MarkerValueSerde implements Serde<MarkerValue> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public void close() {}

        @Override
        public Serializer<MarkerValue> serializer() {
            return new MarkerValueSerializer();
        }

        @Override
        public Deserializer<MarkerValue> deserializer() {
            return new MarkerValueDeserializer();
        }
    };
}
