package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.KmqClient;
import com.softwaremill.kmq.example.UncaughtExceptionHandling;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.config.SslConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.*;

class StandaloneProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneProcessor.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        UncaughtExceptionHandling.setup();
        
        /* EXAMPLE with extraConfig : SSL Encryption & SSL Authentication
        Map extraConfig = new HashMap();
        //configure the following three settings for SSL Encryption
        extraConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        extraConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/directory/kafka.client.truststore.jks");
        extraConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "test1234");

        // configure the following three settings for SSL Authentication
        extraConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/directory/kafka.client.keystore.jks");
        extraConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        extraConfig.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");
        
        KmqClient<ByteBuffer, ByteBuffer> kmqClient = new KmqClient<>(KMQ_CONFIG, KAFKA_CLIENTS,
                ByteBufferDeserializer.class, ByteBufferDeserializer.class, 100, extraConfig);
        */

        KmqClient<ByteBuffer, ByteBuffer> kmqClient = new KmqClient<>(KMQ_CONFIG, KAFKA_CLIENTS,
                ByteBufferDeserializer.class, ByteBufferDeserializer.class, 100);

        ExecutorService msgProcessingExecutor = Executors.newCachedThreadPool();

        while (true) {
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : kmqClient.nextBatch()) {
                msgProcessingExecutor.execute(() -> {
                    if (processMessage(record)) {
                        kmqClient.processed(record);
                    }
                });
            }
        }
    }

    private static Random random = new Random();
    private static Map<Integer, Integer> processedMessages = new ConcurrentHashMap<>();
    private static AtomicInteger totalProcessed = new AtomicInteger(0);
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
            }

            int total = totalProcessed.incrementAndGet();
            LOG.info(String.format("Done processing message: %d. Total processed: %d.", msg, total));

            return true;
        } else {
            LOG.info("Dropping message: " + msg);
            return false;
        }
    }
}
