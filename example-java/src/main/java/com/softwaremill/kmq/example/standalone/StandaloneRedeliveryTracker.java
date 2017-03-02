package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.RedeliveryTracker;
import com.softwaremill.kmq.example.UncaughtExceptionHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.*;

class StandaloneRedeliveryTracker {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneRedeliveryTracker.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        UncaughtExceptionHandling.setup();

        Closeable redelivery = RedeliveryTracker.start(KAFKA_CLIENTS, KMQ_CONFIG);
        LOG.info("Redelivery tracker started");

        System.in.read();

        redelivery.close();
        LOG.info("Redelivery tracker stopped");
    }
}