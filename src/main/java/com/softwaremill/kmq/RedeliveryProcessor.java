package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;

public class RedeliveryProcessor implements Processor<MarkerKey, MarkerValue> {
    private final static Logger LOG = LoggerFactory.getLogger(RedeliveryProcessor.class);

    public final static String STARTED_MARKERS_STORE_NAME = "startedMarkers";

    private final Clock clock = Clock.systemDefaultZone();

    private ProcessorContext context;
    private KeyValueStore<MarkerKey, MarkerValue> startedMarkers;
    private MarkersQueue markersQueue;
    private Closeable closeRedeliveryExecutor;

    private final String msgTopic;
    private final long msgTimeout;
    private final KafkaConsumer<byte[], byte[]> redeliveredMsgsConsumer;
    private final KafkaProducer<byte[], byte[]> redeliveredMsgsProducer;

    public RedeliveryProcessor(String msgTopic, long msgTimeout,
                               KafkaConsumer<byte[], byte[]> redeliveredMsgsConsumer,
                               KafkaProducer<byte[], byte[]> redeliveredMsgsProducer) {
        this.msgTopic = msgTopic;
        this.msgTimeout = msgTimeout;
        this.redeliveredMsgsConsumer = redeliveredMsgsConsumer;
        this.redeliveredMsgsProducer = redeliveredMsgsProducer;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        //noinspection unchecked
        startedMarkers = (KeyValueStore<MarkerKey, MarkerValue>) context.getStateStore("startedMarkers");

        this.markersQueue = new MarkersQueue(k -> startedMarkers.get(k) == null, clock, msgTimeout);
        restoreMarkersQueue();

        RedeliveryExecutor redeliveryExecutor = new RedeliveryExecutor(msgTopic, markersQueue,
                redeliveredMsgsConsumer, redeliveredMsgsProducer,
                // when a message is redelivered, removing it from the store
                k -> { startedMarkers.delete(k); return null; });
        closeRedeliveryExecutor = RedeliveryExecutor.schedule(redeliveryExecutor, 1, TimeUnit.SECONDS);

        LOG.info(String.format("Started new redelivery processor for message topic %s", msgTopic));
    }

    @Override
    public void process(MarkerKey key, MarkerValue value) {
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
        startedMarkers.close();

        if (closeRedeliveryExecutor != null) {
            try { closeRedeliveryExecutor.close(); } catch (IOException e) { throw new RuntimeException(e); }
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
