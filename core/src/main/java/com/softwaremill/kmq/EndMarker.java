package com.softwaremill.kmq;

import java.nio.ByteBuffer;

public class EndMarker implements MarkerValue {
    private EndMarker() {}

    public byte[] serialize() {
        return ByteBuffer.allocate(1)
                .put((byte) 0)
                .array();
    }

    @Override
    public String toString() {
        return "EndMarker{}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public static final EndMarker INSTANCE = new EndMarker();
}
