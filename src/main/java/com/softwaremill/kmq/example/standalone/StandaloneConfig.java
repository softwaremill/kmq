package com.softwaremill.kmq.example.standalone;

import com.softwaremill.kmq.KafkaClients;

class StandaloneConfig {
    static final String MESSAGES_TOPIC = "queue";
    static final String MARKERS_TOPIC = "markers";

    static final KafkaClients KAFKA_CLIENTS = new KafkaClients("localhost:1234");
}
