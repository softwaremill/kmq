package com.softwaremill.kmq;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class MarkerValue {
    private final boolean start;
    private final long redeliverTimestamp;

    public MarkerValue(boolean start, long redeliverTimestamp) {
        this.start = start;
        this.redeliverTimestamp = redeliverTimestamp;
    }

    public boolean isStart() {
        return start;
    }

    public long getRedeliverTimestamp() {
        return redeliverTimestamp;
    }

    public byte[] serialize() {
        return ByteBuffer.allocate(1 + 8)
                .put((byte) (start ? 1 : 0))
                .putLong(redeliverTimestamp)
                .array();
    }

    public MarkerValue asEndMarker() {
        return new MarkerValue(false, redeliverTimestamp);
    }

    @Override
    public String toString() {
        return "MarkerValue{" +
                "start=" + start +
                ", redeliverTimestamp=" + redeliverTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MarkerValue that = (MarkerValue) o;
        return start == that.start &&
                redeliverTimestamp == that.redeliverTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, redeliverTimestamp);
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
}
