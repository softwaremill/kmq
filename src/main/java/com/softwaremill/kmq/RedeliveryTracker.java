package com.softwaremill.kmq;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.Closeable;

public class RedeliveryTracker {
    public static Closeable setup(String dataTopic, String offsetTopic) {
        StateStoreSupplier startedMarkers = Stores.create(RedeliveryProcessor.STARTED_MARKERS_STORE_NAME)
                .withKeys(new MarkerKey.MarkerKeySerde())
                .withValues(new MarkerValue.MarkerValueSerde())
                .persistent()
                //.enableLogging() TODO
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", offsetTopic)
                .addProcessor("process", () -> new RedeliveryProcessor(dataTopic,
                        offsetTopic, KafkaClients.createConsumer(ByteArrayDeserializer.class, ByteArrayDeserializer.class),
                        KafkaClients.createProducer(ByteArraySerializer.class, ByteArraySerializer.class)), "source")
                .addStateStore(startedMarkers, "process");

        KafkaStreams streams = new KafkaStreams(builder, KafkaClients.kafkaStreamsProps("redelivery",
                MarkerKey.MarkerKeySerde.class, MarkerValue.MarkerValueSerde.class));

        streams.start();

        return streams::close;
    }
}
