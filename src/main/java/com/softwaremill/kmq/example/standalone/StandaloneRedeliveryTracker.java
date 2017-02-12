package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.RedeliveryTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.*;

class StandaloneRedeliveryTracker {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneRedeliveryTracker.class);

    private final static long MESSAGE_TIMEOUT = Duration.ofSeconds(30).toMillis();

    public static void main(String[] args) throws InterruptedException, IOException {
        Closeable redelivery = RedeliveryTracker.setup(KAFKA_CLIENTS, MESSAGES_TOPIC, MARKERS_TOPIC, MESSAGE_TIMEOUT);
        LOG.info("Redelivery tracker started");

        System.in.read();

        redelivery.close();
        LOG.info("Redelivery tracker stopped");
    }
}