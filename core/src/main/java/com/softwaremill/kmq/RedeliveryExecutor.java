package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class RedeliveryExecutor {
    private final static Logger LOG = LoggerFactory.getLogger(RedeliveryExecutor.class);

    private final static long POLL_TIMEOUT = Duration.ofSeconds(100).toMillis();
    private final static long SEND_TIMEOUT_SECONDS = 60;

    private final String msgTopic;
    private final MarkersQueue markersQueue;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProducer<byte[], byte[]> producer;
    private final Function<MarkerKey, Void> onMessageRedelivered;

    private int assignedPartition = -1;

    RedeliveryExecutor(String msgTopic, MarkersQueue markersQueue, KafkaConsumer<byte[], byte[]> consumer,
                              KafkaProducer<byte[], byte[]> producer, Function<MarkerKey, Void> onMessageRedelivered) {
        this.msgTopic = msgTopic;
        this.markersQueue = markersQueue;
        this.consumer = consumer;
        this.producer = producer;
        this.onMessageRedelivered = onMessageRedelivered;
    }

    private void redeliverTimedoutMessages() {
        markersQueue.removeEndedMarkers();
        redeliver(markersQueue.markersToRedeliver());
    }

    private void redeliver(List<MarkersQueue.Marker> toRedeliver) {
        toRedeliver.stream()
                .map(m -> new RedeliveredMarker(m, redeliver(m)))
                .forEach(rm -> {
                    try {
                        rm.sendResult.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    onMessageRedelivered.apply(rm.marker.key);
                });
    }

    private Future<RecordMetadata> redeliver(MarkersQueue.Marker marker) {
        ensurePartitionAssigned(marker.key);

        TopicPartition tp = new TopicPartition(msgTopic, marker.key.getPartition());
        // Could be optimized by doing a seek to the first message to redeliver, and then if messages are "close",
        // polling until the right offset is reached.
        consumer.seek(tp, marker.key.getOffset());
        List<ConsumerRecord<byte[], byte[]>> pollResults = consumer.poll(POLL_TIMEOUT).records(tp);
        if (pollResults.isEmpty()) {
            throw new IllegalStateException("Cannot redeliver " + marker.key + " from topic " + msgTopic + ", due to data fetch timeout");
        } else {
            ConsumerRecord<byte[], byte[]> toSend = pollResults.get(0);
            LOG.info(String.format("Redelivering message from %s, partition %d, offset %d", msgTopic, marker.key.getPartition(),
                    marker.key.getOffset()));
            return producer.send(new ProducerRecord<>(
                    toSend.topic(),
                    toSend.partition(),
                    toSend.key(),
                    toSend.value()
            ));
        }
    }

    private static class RedeliveredMarker {
        private final MarkersQueue.Marker marker;
        private final Future<RecordMetadata> sendResult;

        RedeliveredMarker(MarkersQueue.Marker marker, Future<RecordMetadata> sendResult) {
            this.marker = marker;
            this.sendResult = sendResult;
        }
    }

    private void ensurePartitionAssigned(MarkerKey key) {
        if (assignedPartition == -1) {
            assignedPartition = key.getPartition();
            consumer.assign(Collections.singleton(new TopicPartition(msgTopic, assignedPartition)));

            LOG.info(String.format("Assigned partition %d.", assignedPartition));
        } else {
            if (key.getPartition() != assignedPartition) {
                throw new IllegalStateException(
                        String.format("Got marker key for partition %d, while the assigned partition is %d!",
                                key.getPartition(), assignedPartition));
            }
        }
    }

    static Runnable schedule(RedeliveryExecutor executor, int every, TimeUnit timeUnit) {
        ScheduledExecutorService punctuateExecutor = Executors.newSingleThreadScheduledExecutor();
        punctuateExecutor.scheduleAtFixedRate(
                () -> {
                    try { executor.redeliverTimedoutMessages(); }
                    catch (Exception e) {
                        Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
                    }
                },
                every, every, timeUnit
        );
        return () -> {
            punctuateExecutor.shutdown();
            try {
                punctuateExecutor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) { throw new RuntimeException(e); }
        };
    }
}
