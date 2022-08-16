package com.softwaremill.kmq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private static final int DEFAULT_MAX_REDELIVERY_COUNT = 3;
    private static final String DEFAULT_REDELIVERY_COUNT_HEADER = "kmq-redelivery-count";
    private static final String DEFAULT_DEAD_LETTER_TOPIC_SUFFIX = "__undelivered";

    private final String bootstrapServers;
    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryConsumerGroupId;
    private final long msgTimeoutMs;
    private final long useNowForRedeliverDespiteNoMarkerSeenForMs;
    private final String deadLetterTopic;
    private final String redeliveryCountHeader;
    private final int maxRedeliveryCount;
    private final Map<String, String> extraGlobalConfig;

    /**
     * @param bootstrapServers Kafka bootstrap servers.
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param redeliveryConsumerGroupId Consumer group id for reading messages from `markerTopic`.
     * @param msgTimeoutMs Timeout, after which messages, if not processed, are redelivered.
     * @param useNowForRedeliverDespiteNoMarkerSeenForMs After what time "now" should be use to calculate redelivery
     *                                                   instead of maximum marker timestamp seen in a partition
     * @param deadLetterTopic Name of the Kafka topic containing all undelivered messages.
     * @param redeliveryCountHeader Name of the redelivery count header.
     * @param maxRedeliveryCount Max number of message redeliveries.
     * @param extraGlobalConfig Extra Kafka parameter configuration, e.g. SSL.
     */
    public KmqConfig(
            String bootstrapServers,
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs,
            String deadLetterTopic, String redeliveryCountHeader, int maxRedeliveryCount,
            Map<String, String> extraGlobalConfig) {
        this.bootstrapServers = bootstrapServers;
        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.redeliveryConsumerGroupId = redeliveryConsumerGroupId;
        this.msgTimeoutMs = msgTimeoutMs;
        this.useNowForRedeliverDespiteNoMarkerSeenForMs = useNowForRedeliverDespiteNoMarkerSeenForMs;
        this.deadLetterTopic = deadLetterTopic;
        this.redeliveryCountHeader = redeliveryCountHeader;
        this.maxRedeliveryCount = maxRedeliveryCount;
        this.extraGlobalConfig = extraGlobalConfig;
    }

    public KmqConfig(
            String bootstrapServers,
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs) {

        this(bootstrapServers, msgTopic, markerTopic, msgConsumerGroupId, redeliveryConsumerGroupId,
                msgTimeoutMs, useNowForRedeliverDespiteNoMarkerSeenForMs,
                msgTopic + DEFAULT_DEAD_LETTER_TOPIC_SUFFIX, DEFAULT_REDELIVERY_COUNT_HEADER, DEFAULT_MAX_REDELIVERY_COUNT,
                Collections.emptyMap());
    }

    public String getMsgTopic() {
        return msgTopic;
    }

    public String getMarkerTopic() {
        return markerTopic;
    }

    public String getMsgConsumerGroupId() {
        return msgConsumerGroupId;
    }

    public String getRedeliveryConsumerGroupId() {
        return redeliveryConsumerGroupId;
    }

    public long getMsgTimeoutMs() {
        return msgTimeoutMs;
    }

    public long getUseNowForRedeliverDespiteNoMarkerSeenForMs() {
        return useNowForRedeliverDespiteNoMarkerSeenForMs;
    }

    public String getDeadLetterTopic() {
        return deadLetterTopic;
    }

    public String getRedeliveryCountHeader() {
        return redeliveryCountHeader;
    }

    public int getMaxRedeliveryCount() {
        return maxRedeliveryCount;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public Map<String, String> getProducerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        props.putAll(extraGlobalConfig);
        return props;
    }

    public Map<String, String> getConsumerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(extraGlobalConfig);
        return props;
    }
}
