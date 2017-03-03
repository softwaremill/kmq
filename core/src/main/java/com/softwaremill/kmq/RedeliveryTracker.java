package com.softwaremill.kmq;

import com.softwaremill.kmq.redelivery.RedeliveryActors;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.Closeable;

/**
 * Tracks which messages has been processed, and redelivers the ones which are not processed during the
 * configured timeout.
 */
public class RedeliveryTracker {
    public static Closeable start(KafkaClients clients, KmqConfig config) {
        return RedeliveryActors.start(clients, config);
    }
}
