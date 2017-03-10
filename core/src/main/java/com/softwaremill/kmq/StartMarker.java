package com.softwaremill.kmq;

import java.nio.ByteBuffer;
import java.util.Objects;

public class StartMarker implements MarkerValue {
    private final long redeliverAfter;

    public StartMarker(long redeliverAfter) {
        this.redeliverAfter = redeliverAfter;
    }

    public long getRedeliverAfter() {
        return redeliverAfter;
    }

    public byte[] serialize() {
        return ByteBuffer.allocate(1 + 8)
                .put((byte) 1)
                .putLong(redeliverAfter)
                .array();
    }

    @Override
    public String toString() {
        return "StartMarker{" +
                "redeliverAfter=" + redeliverAfter +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StartMarker that = (StartMarker) o;
        return redeliverAfter == that.redeliverAfter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(redeliverAfter);
    }
}
