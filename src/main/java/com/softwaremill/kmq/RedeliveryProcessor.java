package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RedeliveryProcessor implements Processor<MarkerKey, MarkerValue> {
    private final static Logger LOG = LoggerFactory.getLogger(RedeliveryProcessor.class);

    public final static String STARTED_MARKERS_STORE_NAME = "startedMarkers";

    private final static long PUNCTUATE_OFFSET = -1;

    private final static long POLL_TIMEOUT = Duration.ofSeconds(10).toMillis();
    private final static long MESSAGE_TIMEOUT = Duration.ofSeconds(30).toMillis();
    private final Clock clock = Clock.systemDefaultZone();

    private ProcessorContext context;
    private KeyValueStore<MarkerKey, MarkerValue> startedMarkers;
    private MarkersQueue markersQueue;
    private int assignedPartition = -1;
    private Closeable closePunctuateSender;

    private final String dataTopic;
    private final String offsetTopic;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProducer<byte[], byte[]> producer;

    public RedeliveryProcessor(String dataTopic, String offsetTopic, KafkaConsumer<byte[], byte[]> consumer,
                               KafkaProducer<byte[], byte[]> producer) {
        this.dataTopic = dataTopic;
        this.offsetTopic = offsetTopic;
        this.consumer = consumer;
        this.producer = producer;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        //noinspection unchecked
        startedMarkers = (KeyValueStore<MarkerKey, MarkerValue>) context.getStateStore("startedMarkers");

        this.markersQueue = new MarkersQueue(k -> startedMarkers.get(k) == null, clock, MESSAGE_TIMEOUT);

        restoreMarkersQueue();
    }

    @Override
    public void process(MarkerKey key, MarkerValue value) {
        ensurePartitionAssigned(key);

        if (key.getOffset() == PUNCTUATE_OFFSET) {
            // The built-in punctuate is only called when event-time progresses. If there are no new markers arriving,
            // punctuate(long) won't be called at all. Hence using a custom punctuate mechanism using special markers
            // sent by a background thread every second.
            punctuate();
        } else if (value.isStart()) {
            startedMarkers.put(key, value);
            markersQueue.offer(key, value);
        } else {
            startedMarkers.delete(key);
        }

        context.commit();
    }

    private void ensurePartitionAssigned(MarkerKey key) {
        if (assignedPartition == -1) {
            assignedPartition = key.getPartition();
            consumer.assign(Collections.singleton(new TopicPartition(dataTopic, assignedPartition)));

            LOG.info(String.format("Assigned partition %d.", assignedPartition));

            schedulePunctuateSends();
        } else {
            if (key.getPartition() != assignedPartition) {
                throw new IllegalStateException(
                        String.format("Got marker key for partition %d, while the assigned partition is %d!",
                                key.getPartition(), assignedPartition));
            }
        }
    }

    private void schedulePunctuateSends() {
        final ProducerRecord<byte[], byte[]> punctuateMsg = new ProducerRecord<>(offsetTopic,
                new MarkerKey(assignedPartition, PUNCTUATE_OFFSET).serialize(),
                new MarkerValue(true, 0).serialize());
        ScheduledExecutorService punctuateExecutor = Executors.newSingleThreadScheduledExecutor();
        punctuateExecutor.scheduleAtFixedRate(
                () -> producer.send(punctuateMsg),
                1, 1, TimeUnit.SECONDS
        );
        closePunctuateSender = punctuateExecutor::shutdown;
    }

    @Override
    public void punctuate(long timestamp) {}

    private void punctuate() {
        markersQueue.removeEndedMarkers();
        redeliverTimedoutMessages();
        context.commit();
    }

    @Override
    public void close() {
        LOG.info("Closing redelivery processor");
        startedMarkers.close();

        if (closePunctuateSender != null) {
            try { closePunctuateSender.close(); } catch (IOException e) { throw new RuntimeException(e); }
        }
    }

    private void redeliverTimedoutMessages() {
        redeliver(markersQueue.markersToRedeliver());
    }

    private void redeliver(List<MarkersQueue.Marker> toRedeliver) {
        toRedeliver.stream()
                .map(m -> new RedeliveredMarker(m, redeliver(m)))
                .forEach(rm -> {
                    try {
                        rm.sendResult.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    // now that we know the message has been redelivered, we can remove it from the store
                    startedMarkers.delete(rm.marker.key);
                });
    }

    private Future<RecordMetadata> redeliver(MarkersQueue.Marker marker) {
        TopicPartition tp = new TopicPartition(dataTopic, marker.key.getPartition());
        // Could be optimized by doing a seek to the first message to redeliver, and then if messages are "close",
        // polling until the right offset is reached.
        consumer.seek(tp, marker.key.getOffset());
        List<ConsumerRecord<byte[], byte[]>> pollResults = consumer.poll(POLL_TIMEOUT).records(tp);
        if (pollResults.isEmpty()) {
            throw new IllegalStateException("Cannot redeliver " + marker.key + " from topic " + dataTopic + ", due to data fetch timeout");
        } else {
            ConsumerRecord<byte[], byte[]> toSend = pollResults.get(0);
            LOG.info("Redelivering " + marker.key.getOffset());
            return producer.send(new ProducerRecord<>(
                    toSend.topic(),
                    toSend.partition(),
                    toSend.key(),
                    toSend.value()
            ));
        }
    }

    private void restoreMarkersQueue() {
        KeyValueIterator<MarkerKey, MarkerValue> allIterator = startedMarkers.all();
        allIterator.forEachRemaining(kv -> {
            if (kv.value != null) markersQueue.offer(kv.key, kv.value);
        });
        allIterator.close();
    }

    private static class RedeliveredMarker {
        private final MarkersQueue.Marker marker;
        private final Future<RecordMetadata> sendResult;

        public RedeliveredMarker(MarkersQueue.Marker marker, Future<RecordMetadata> sendResult) {
            this.marker = marker;
            this.sendResult = sendResult;
        }
    }

}
