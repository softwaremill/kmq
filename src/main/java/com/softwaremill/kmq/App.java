package com.softwaremill.kmq;

import net.manub.embeddedkafka.EmbeddedKafka$;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map$;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.softwaremill.kmq.KafkaClients.*;

public class App {
    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    public static final String QUEUE = "queue";
    public static final String OFFSETS = "offsets";

    private static final Clock clock = Clock.systemDefaultZone();

    private static KafkaProducer<MarkerKey, MarkerValue> offsetProducer;
    private static KafkaProducer<ByteBuffer, ByteBuffer> msgProducer;
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public static void main(String[] args) throws InterruptedException, IOException {
        int partitions = 1;

        EmbeddedKafka$.MODULE$.start(EMBEDDED_KAFKA_CONFIG);
        // The offsets topic has the same # of partitions as the queue topic.
        EmbeddedKafka$.MODULE$.createCustomTopic(QUEUE, Map$.MODULE$.empty(), partitions, 1, EMBEDDED_KAFKA_CONFIG);
        EmbeddedKafka$.MODULE$.createCustomTopic(OFFSETS, Map$.MODULE$.empty(), partitions, 1, EMBEDDED_KAFKA_CONFIG);
        LOG.info("Kafka started");

        // Using the custom partitioner, each offset-partition will contain markers only from a single queue-partition.
        offsetProducer = createProducer(MarkerKey.MarkerKeySerializer.class, MarkerValue.MarkerValueSerializer.class,
                Collections.singletonMap(ProducerConfig.PARTITIONER_CLASS_CONFIG, ParititionFromMarkerKey.class));
        msgProducer = createProducer(ByteBufferSerializer.class, ByteBufferSerializer.class);

        startInBackground(App::sendMessages);
        startInBackground(App::processMessages);
        Closeable redelivery = RedeliveryTracker.setup(QUEUE, OFFSETS);

        System.in.read();

        redelivery.close();
        EmbeddedKafka$.MODULE$.stop();
        LOG.info("Kafka stopped");
    }

    private static void processMessages() {
        KafkaConsumer<ByteBuffer, ByteBuffer> msgConsumer = createConsumer(ByteBufferDeserializer.class, ByteBufferDeserializer.class);
        msgConsumer.subscribe(Collections.singletonList(QUEUE));

        LOG.info("Processing ...");

        List<Future<RecordMetadata>> markerSends = new ArrayList<>();
        while (true) {
            // 1. Get messages from topic, in batches
            ConsumerRecords<ByteBuffer, ByteBuffer> records = msgConsumer.poll(100);
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                // 2. Write a "start" marker. Collecting the future responses.
                markerSends.add(offsetProducer.send(
                        new ProducerRecord<>(OFFSETS,
                                MarkerKey.fromRecord(record),
                                new MarkerValue(true, clock.millis()))));
            }

            // Waiting for a confirmation that each start marker has been sent
            markerSends.forEach(f -> {
                try { f.get(); } catch (Exception e) { throw new RuntimeException(e); }
            });

            // 3. Now that we now the start markers have been sent, we can start processing the data
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                executorService.execute(processDataRunnable(record.value(), MarkerKey.fromRecord(record)));
            }

            // 4. after all start markers are sent, commit offsets. This need to be done as close to writing the
            // start marker as possible, to minimize the number of double re-processed messages in case of failure.
            msgConsumer.commitSync();
        }
    }

    private static void sendMessages() {
        LOG.info("Sending ...");

        for(int i = 0; i < 100; i++) {
            ByteBuffer data = ByteBuffer.allocate(4).putInt(i);
            msgProducer.send(new ProducerRecord<>(QUEUE, data));
            try { Thread.sleep(1000L); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }

        LOG.info("Sent");
    }

    private static Random random = new Random();
    private static Runnable processDataRunnable(ByteBuffer data, MarkerKey markerKey) {
        return () -> {
            int msg = data.getInt();
            // 10% of the messages are dropped
            if (random.nextInt(10) != 0) {
                // Sleeping up to 2.5 seconds
                LOG.info("Processing message: " + msg);
                try {
                    Thread.sleep(random.nextInt(25)*100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // 5. writing an "end" marker. No need to wait for confirmation that it has been sent. It would be
                // nice, though, not to ignore that output completely.
                offsetProducer.send(new ProducerRecord<>(OFFSETS,
                        markerKey,
                        new MarkerValue(false, clock.millis())));
                LOG.info("Done processing message: " + msg);
            } else {
                LOG.info("Dropping message: " + msg);
            }
        };
    }

    private static void startInBackground(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();
    }
}