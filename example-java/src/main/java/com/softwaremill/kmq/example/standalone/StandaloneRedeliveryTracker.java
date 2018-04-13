package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.RedeliveryTracker;
import com.softwaremill.kmq.example.UncaughtExceptionHandling;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.softwaremill.kmq.example.standalone.StandaloneConfig.*;

class StandaloneRedeliveryTracker {
    private final static Logger LOG = LoggerFactory.getLogger(StandaloneRedeliveryTracker.class);

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
        
        Closeable redelivery = RedeliveryTracker.start(KAFKA_CLIENTS, KMQ_CONFIG, scala.Option.apply(extraConfig));
        */

        Closeable redelivery = RedeliveryTracker.start(KAFKA_CLIENTS, KMQ_CONFIG);
        LOG.info("Redelivery tracker started");

        System.in.read();

        redelivery.close();
        LOG.info("Redelivery tracker stopped");
    }
}
