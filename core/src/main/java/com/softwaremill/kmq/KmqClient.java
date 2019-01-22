package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Kafka-based MQ client. A call to `nextBatch()` will:
 * 1. poll the `msgTopic` for at most `msgPollTimeout`
 * 2. send the start markers to the `markerTopic`, wait until they are written
 * 3. commit read offsets of the messages from `msgTopic`
 *
 * The next step of the message flow - 4. processing the messages - should be done by the client.
 *
 * After each message is processed, the `processed()` method should be called, which will:
 * 5. send an end marker to the `markerTopic`
 *
 * Note that `processed()` can be called at any time for any message and out-of-order. If processing fails, it shuoldn't
 * be called at all.
 */
public class KmqClient<K, V> implements Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(KmqClient.class);

    private final KmqConfig config;
    private final Duration msgPollTimeout;

    private final KafkaConsumer<K, V> msgConsumer;
    private final KafkaProducer<MarkerKey, MarkerValue> markerProducer;

    public KmqClient(KmqConfig config, KafkaClients clients,
                     Class<? extends Deserializer<K>> keyDeserializer,
                     Class<? extends Deserializer<V>> valueDeserializer,
                     Duration msgPollTimeout) {

        this.config = config;
        this.msgPollTimeout = msgPollTimeout;

        this.msgConsumer = clients.createConsumer(config.getMsgConsumerGroupId(), keyDeserializer, valueDeserializer);
        // Using the custom partitioner, each offset-partition will contain markers only from a single queue-partition.
        this.markerProducer = clients.createProducer(
                MarkerKey.MarkerKeySerializer.class, MarkerValue.MarkerValueSerializer.class,
                Collections.singletonMap(ProducerConfig.PARTITIONER_CLASS_CONFIG, ParititionFromMarkerKey.class));

        LOG.info(String.format("Subscribing to topic: %s, using group id: %s", config.getMsgTopic(), config.getMsgConsumerGroupId()));
        msgConsumer.subscribe(Collections.singletonList(config.getMsgTopic()));
    }

    public ConsumerRecords<K, V> nextBatch() {
        List<Future<RecordMetadata>> markerSends = new ArrayList<>();

        // 1. Get messages from topic, in batches
        ConsumerRecords<K, V> records = msgConsumer.poll(msgPollTimeout);
        for (ConsumerRecord<K, V> record : records) {
            // 2. Write a "start" marker. Collecting the future responses.
            markerSends.add(markerProducer.send(
                    new ProducerRecord<>(config.getMarkerTopic(),
                            MarkerKey.fromRecord(record),
                            new StartMarker(config.getMsgTimeoutMs()))));
        }

        // Waiting for a confirmation that each start marker has been sent
        markerSends.forEach(f -> {
            try { f.get(); } catch (Exception e) { throw new RuntimeException(e); }
        });

        // 3. after all start markers are sent, commit offsets. This needs to be done as close to writing the
        // start marker as possible, to minimize the number of double re-processed messages in case of failure.
        msgConsumer.commitSync();

        return records;
    }

    // client-side: 4. process the messages

    /**
     * @param record The message for which should be acknowledged as processed; an end marker will be send to the
     *               markers topic.
     * @return Result of the marker send. Usually can be ignored, we don't need a guarantee the marker has been sent,
     * worst case the message will be reprocessed.
     */
    public Future<RecordMetadata> processed(ConsumerRecord<K, V> record) {
        // 5. writing an "end" marker. No need to wait for confirmation that it has been sent. It would be
        // nice, though, not to ignore that output completely.
        return markerProducer.send(new ProducerRecord<>(config.getMarkerTopic(),
                MarkerKey.fromRecord(record),
                EndMarker.INSTANCE));
    }

    @Override
    public void close() throws IOException {
        msgConsumer.close();
        markerProducer.close();
    }
}
