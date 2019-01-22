package com.softwaremill.kmq;

import com.softwaremill.kmq.redelivery.RedeliveryActors;

import scala.Option;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/**
 * Tracks which messages has been processed, and redelivers the ones which are not processed until their redelivery
 * time.
 */
public class RedeliveryTracker {
    public static Closeable start(KafkaClients clients, KmqConfig config) {
        return RedeliveryActors.start(clients, config);
    }
}
