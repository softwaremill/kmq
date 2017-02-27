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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Kafka-based MQ client, processes messages from the message topic using the given function, sending appropriate
 * start and end markers before and after a message is processed, to ensure redelivery in case of processing failure.
 */
public class KmqClient<K, V> {
    private final static Logger LOG = LoggerFactory.getLogger(KmqClient.class);

    private final KmqConfig config;
    private final Function<ConsumerRecord<K, V>, Boolean> processData;
    private final Clock clock;

    private final KafkaConsumer<K, V> msgConsumer;
    private final KafkaProducer<MarkerKey, MarkerValue> markerProducer;

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public KmqClient(KmqConfig config, Function<ConsumerRecord<K, V>, Boolean> processMsg,
                     Clock clock, KafkaClients clients, Class<? extends Deserializer<K>> keyDeserializer,
                     Class<? extends Deserializer<V>> valueDeserializer) {

        this.config = config;
        this.processData = processMsg;
        this.clock = clock;

        this.msgConsumer = clients.createConsumer(config.getMsgConsumerGroupId(), keyDeserializer, valueDeserializer);
        // Using the custom partitioner, each offset-partition will contain markers only from a single queue-partition.
        this.markerProducer = clients.createProducer(
                MarkerKey.MarkerKeySerializer.class, MarkerValue.MarkerValueSerializer.class,
                Collections.singletonMap(ProducerConfig.PARTITIONER_CLASS_CONFIG, ParititionFromMarkerKey.class));
    }

    public void start() {
        msgConsumer.subscribe(Collections.singletonList(config.getMsgTopic()));

        LOG.info("Starting KMQ Java client");

        List<Future<RecordMetadata>> markerSends = new ArrayList<>();
        while (true) {
            // 1. Get messages from topic, in batches
            ConsumerRecords<K, V> records = msgConsumer.poll(100);
            for (ConsumerRecord<K, V> record : records) {
                // 2. Write a "start" marker. Collecting the future responses.
                markerSends.add(markerProducer.send(
                        new ProducerRecord<>(config.getMarkerTopic(),
                                MarkerKey.fromRecord(record),
                                new MarkerValue(true, clock.millis()))));
            }

            // Waiting for a confirmation that each start marker has been sent
            markerSends.forEach(f -> {
                try { f.get(); } catch (Exception e) { throw new RuntimeException(e); }
            });

            // 3. after all start markers are sent, commit offsets. This needs to be done as close to writing the
            // start marker as possible, to minimize the number of double re-processed messages in case of failure.
            msgConsumer.commitSync();

            // 4. Now that we now the start markers have been sent, we can start processing the messages
            for (ConsumerRecord<K, V> record : records) {
                executorService.execute(processDataRunnable(record, MarkerKey.fromRecord(record)));
            }
        }
    }

    private Runnable processDataRunnable(ConsumerRecord<K, V> msg, MarkerKey markerKey) {
        return () -> {
            if (processData.apply(msg)) {
                // 5. writing an "end" marker. No need to wait for confirmation that it has been sent. It would be
                // nice, though, not to ignore that output completely.
                markerProducer.send(new ProducerRecord<>(config.getMarkerTopic(),
                        markerKey,
                        new MarkerValue(false, clock.millis())));
            }
        };
    }
}
