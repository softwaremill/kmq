package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.KafkaClients;

class StandaloneConfig {
    static final String KMQ_CLIENT_GROUP_ID = "kmq_client";
    static final String REDELIVERY_APP_ID = "kmq_redelivery";
    static final String MESSAGES_TOPIC = "queue";
    static final String MARKERS_TOPIC = "markers";

    static final KafkaClients KAFKA_CLIENTS = new KafkaClients("localhost:1234");
}
