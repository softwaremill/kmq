package com.softwaremill.kmq;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public interface MarkerValue {
    byte[] serialize();

    class MarkerValueSerializer implements Serializer<MarkerValue> {
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

    class MarkerValueDeserializer implements Deserializer<MarkerValue> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public MarkerValue deserialize(String topic, byte[] data) {
            if (data.length == 0) {
                return null;
            } else {
                ByteBuffer bb = ByteBuffer.wrap(data);
                if (bb.get() == 1) {
                    return new StartMarker(bb.getLong());
                } else {
                    return EndMarker.INSTANCE;
                }
            }
        }

        @Override
        public void close() {}
    }
}
