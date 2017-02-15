package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.example.UncaughtExceptionHandling;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.KAFKA_CLIENTS;
import static com.softwaremill.kmq.example.standalone.StandaloneConfig.KMQ_CONFIG;

class StandaloneSender {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneSender.class);

    static final int TOTAL_MSGS = 100;

    public static void main(String[] args) throws InterruptedException, IOException {
        UncaughtExceptionHandling.setup();

        KafkaProducer<ByteBuffer, ByteBuffer> msgProducer = KAFKA_CLIENTS
                .createProducer(ByteBufferSerializer.class, ByteBufferSerializer.class);

        LOG.info("Sending ...");

        for(int i = 0; i < TOTAL_MSGS; i++) {
            ByteBuffer data = ByteBuffer.allocate(4).putInt(i);
            msgProducer.send(new ProducerRecord<>(KMQ_CONFIG.getMsgTopic(), data));
            try { Thread.sleep(100L); } catch (InterruptedException e) { throw new RuntimeException(e); }
            LOG.info(String.format("Sent message %d", i));
        }

        msgProducer.close();

        LOG.info("Sent");
    }
}
