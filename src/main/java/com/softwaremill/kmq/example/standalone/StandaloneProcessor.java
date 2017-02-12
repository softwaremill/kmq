package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.KmqClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.*;
import static com.softwaremill.kmq.example.standalone.StandaloneSender.TOTAL_MSGS;

class StandaloneProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneProcessor.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        KmqClient<ByteBuffer, ByteBuffer> kmqClient = new KmqClient<>(KMQ_CLIENT_GROUP_ID, MESSAGES_TOPIC, MARKERS_TOPIC,
                StandaloneProcessor::processMessage, Clock.systemDefaultZone(), KAFKA_CLIENTS,
                ByteBufferDeserializer.class, ByteBufferDeserializer.class);

        LOG.info("Starting KMQ client");
        kmqClient.start();
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
}
