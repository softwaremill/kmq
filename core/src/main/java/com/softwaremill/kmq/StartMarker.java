package com.softwaremill.kmq;

import java.nio.ByteBuffer;
import java.util.Objects;

public class StartMarker implements MarkerValue {
    private final long redeliverTimestamp;

    public StartMarker(long redeliverTimestamp) {
        this.redeliverTimestamp = redeliverTimestamp;
    }

    public long getRedeliverTimestamp() {
        return redeliverTimestamp;
    }

    public byte[] serialize() {
        return ByteBuffer.allocate(1 + 8)
                .put((byte) 1)
                .putLong(redeliverTimestamp)
                .array();
    }

    @Override
    public String toString() {
        return "StartMarker{" +
                "redeliverTimestamp=" + redeliverTimestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StartMarker that = (StartMarker) o;
        return redeliverTimestamp == that.redeliverTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(redeliverTimestamp);
    }
}
