package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

class RedeliveryProcessor implements Processor<MarkerKey, MarkerValue> {
    private final static Logger LOG = LoggerFactory.getLogger(RedeliveryProcessor.class);

    private final Clock clock = Clock.systemDefaultZone();

    private ProcessorContext context;
    private KeyValueStore<MarkerKey, MarkerValue> startedMarkers;
    private MarkersQueue markersQueue;
    private Runnable closeRedeliveryExecutor;
    private ConcurrentLinkedQueue<MarkerKey> toDelete;

    private final KmqConfig config;
    private final KafkaConsumer<byte[], byte[]> redeliveredMsgsConsumer;
    private final KafkaProducer<byte[], byte[]> redeliveredMsgsProducer;

    RedeliveryProcessor(KmqConfig config,
                               KafkaConsumer<byte[], byte[]> redeliveredMsgsConsumer,
                               KafkaProducer<byte[], byte[]> redeliveredMsgsProducer) {
        this.config = config;
        this.redeliveredMsgsConsumer = redeliveredMsgsConsumer;
        this.redeliveredMsgsProducer = redeliveredMsgsProducer;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        //noinspection unchecked
        startedMarkers = (KeyValueStore<MarkerKey, MarkerValue>) context.getStateStore(config.getStartedMarkersStoreName());

        this.markersQueue = new MarkersQueue(k -> startedMarkers.get(k) == null, clock, config.getMsgTimeout());
        restoreMarkersQueue();

        // keys from "startedMarkers" can only be removed when processing a message (not on another thread), hence when
        // a message is re-delivered its key is added to this collection, and removed later when any message is
        // processed (e.g. the marker key sent by processing the re-delivered message).
        toDelete = new ConcurrentLinkedQueue<>();

        RedeliveryExecutor redeliveryExecutor = new RedeliveryExecutor(config.getMsgTopic(), markersQueue,
                redeliveredMsgsConsumer, redeliveredMsgsProducer,
                // when a message is redelivered, marking it to be removed from the store
                k -> { toDelete.offer(k); return null; });
        closeRedeliveryExecutor = RedeliveryExecutor.schedule(redeliveryExecutor, 1, TimeUnit.SECONDS);

        LOG.info(String.format("Started new redelivery processor for message topic %s", config.getMsgTopic()));
    }

    @Override
    public void process(MarkerKey key, MarkerValue value) {
        deleteKeysToDelete();

        if (value.isStart()) {
            startedMarkers.put(key, value);
            markersQueue.offer(key, value);
        } else {
            startedMarkers.delete(key);
        }

        context.commit();
    }

    @Override
    public void punctuate(long timestamp) {}

    @Override
    public void close() {
        LOG.info("Closing redelivery processor");

        if (closeRedeliveryExecutor != null) {
            closeRedeliveryExecutor.run();
        }
    }

    private void deleteKeysToDelete() {
        MarkerKey keyToDelete;
        while ((keyToDelete = toDelete.poll()) != null) {
            startedMarkers.delete(keyToDelete);
        }
    }

    private void restoreMarkersQueue() {
        KeyValueIterator<MarkerKey, MarkerValue> allIterator = startedMarkers.all();
        allIterator.forEachRemaining(kv -> {
            if (kv.value != null) markersQueue.offer(kv.key, kv.value);
        });
        allIterator.close();
    }
}
