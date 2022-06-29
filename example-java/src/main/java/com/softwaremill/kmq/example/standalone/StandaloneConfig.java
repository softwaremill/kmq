package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.KafkaClients;
import com.softwaremill.kmq.KmqConfig;

import java.time.Duration;

class StandaloneConfig {
    static final KmqConfig KMQ_CONFIG = new KmqConfig("queue", "markers", "kmq_client",
            "kmq_redelivery", Duration.ofSeconds(90).toMillis(), 1000);

    /* TODO: EXAMPLE with extraConfig : SSL Encryption & SSL Authentication

        Map extraConfig = new HashMap();
        //configure the following three settings for SSL Encryption
        extraConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        extraConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/directory/kafka.client.truststore.jks");
        extraConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "test1234");

        // configure the following three settings for SSL Authentication
        extraConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/directory/kafka.client.keystore.jks");
        extraConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
        extraConfig.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");

        static final KafkaClients KAFKA_CLIENTS = new KafkaClients("localhost:9092", extraConfig);
    */

    static final KafkaClients KAFKA_CLIENTS = new KafkaClients("localhost:9092");
}
