package com.softwaremill.kmq;

import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.concurrent.ConcurrentHashMap;

public class App {
    private final static Logger LOG = LoggerFactory.getLogger(App.class);

    public static final String MESSAGES_TOPIC = "queue";
    public static final String MARKERS_TOPIC = "markers";

    private static final Clock clock = Clock.systemDefaultZone();

    private static final int PARTITIONS = 1;
    private static final int TOTAL_MSGS = 100;

    public static void main(String[] args) throws InterruptedException, IOException {
        EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.defaultConfig();
        KafkaClients clients = new KafkaClients("localhost:" + kafkaConfig.kafkaPort());

        EmbeddedKafka$.MODULE$.start(kafkaConfig);
        // The offsets topic has the same # of partitions as the queue topic.
        EmbeddedKafka$.MODULE$.createCustomTopic(MESSAGES_TOPIC, Map$.MODULE$.empty(), PARTITIONS, 1, kafkaConfig);
        EmbeddedKafka$.MODULE$.createCustomTopic(MARKERS_TOPIC, Map$.MODULE$.empty(), PARTITIONS, 1, kafkaConfig);
        LOG.info("Kafka started");

        KmqClient<ByteBuffer, ByteBuffer> kmqClient = new KmqClient<>(MESSAGES_TOPIC, MARKERS_TOPIC,
                App::processMessage, clock, clients,
                ByteBufferDeserializer.class, ByteBufferDeserializer.class);

        Closeable redelivery = RedeliveryTracker.setup(clients, MESSAGES_TOPIC, MARKERS_TOPIC);
        startInBackground(kmqClient::start);
        startInBackground(() -> sendMessages(clients));

        System.in.read();

        redelivery.close();
        EmbeddedKafka$.MODULE$.stop();
        LOG.info("Kafka stopped");
    }

    private static void sendMessages(KafkaClients clients) {
        KafkaProducer<ByteBuffer, ByteBuffer> msgProducer = clients.createProducer(ByteBufferSerializer.class, ByteBufferSerializer.class);

        LOG.info("Sending ...");

        for(int i = 0; i < TOTAL_MSGS; i++) {
            ByteBuffer data = ByteBuffer.allocate(4).putInt(i);
            msgProducer.send(new ProducerRecord<>(MESSAGES_TOPIC, data));
            try { Thread.sleep(1000L); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }

        msgProducer.close();

        LOG.info("Sent");
    }

    private static Random random = new Random();
    private static Map<Integer, Integer> processedMessages = new ConcurrentHashMap<>();
    private static boolean processMessage(ConsumerRecord<ByteBuffer, ByteBuffer> rawMsg) {
        int msg = rawMsg.value().getInt();
        // 10% of the messages are dropped
        if (random.nextInt(10) != 0) {
            // Sleeping up to 2.5 seconds
            LOG.info("Processing message: " + msg);
            try {
                Thread.sleep(random.nextInt(25)*100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Integer previous = processedMessages.put(msg, msg);
            if (previous != null) {
                LOG.warn(String.format("Message %d was already processed!", msg));
            } else {
                LOG.info(String.format("Done processing message: %d. Total processed: %d/%d.",
                        msg, processedMessages.size(), TOTAL_MSGS));
            }

            return true;
        } else {
            LOG.info("Dropping message: " + msg);
            return false;
        }
    }

    // ---

    private static void startInBackground(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();
    }
}