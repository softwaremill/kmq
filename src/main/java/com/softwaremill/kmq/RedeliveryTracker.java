package com.softwaremill.kmq;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.Closeable;

public class RedeliveryTracker {
    public static Closeable setup(KafkaClients clients, KmqConfig config) {

        StateStoreSupplier startedMarkers = Stores.create(config.getStartedMarkersStoreName())
                .withKeys(new MarkerKey.MarkerKeySerde())
                .withValues(new MarkerValue.MarkerValueSerde())
                .persistent()
                //.enableLogging() TODO
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", config.getMarkerTopic())
                .addProcessor("process", () -> new RedeliveryProcessor(config,
                        clients.createConsumer(null, ByteArrayDeserializer.class, ByteArrayDeserializer.class),
                        clients.createProducer(ByteArraySerializer.class, ByteArraySerializer.class)), "source")
                .addStateStore(startedMarkers, "process");

        KafkaStreams streams = new KafkaStreams(builder, clients.streamsProps(config.getRedeliveryAppId(),
                MarkerKey.MarkerKeySerde.class, MarkerValue.MarkerValueSerde.class));

        streams.start();

        return streams::close;
    }
}
