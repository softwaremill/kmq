package com.softwaremill.kmq;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private static final int DEFAULT_MAX_REDELIVERY_COUNT = 3;
    private static final String DEFAULT_REDELIVERY_COUNT_HEADER = "kmq-redelivery-count";
    private static final String DEFAULT_DEAD_LETTER_TOPIC_SUFFIX = "__undelivered";

    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String markerConsumerGroupId;
    private final String markerConsumerOffsetGroupId;
    private final long msgTimeoutMs;
    private final long useNowForRedeliverDespiteNoMarkerSeenForMs;
    private final String deadLetterTopic;
    private final String redeliveryCountHeader;
    private final int maxRedeliveryCount;

    /**
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param markerConsumerGroupId Consumer group id for reading messages from `markerTopic`.
     * @param markerConsumerOffsetGroupId Consumer group id for direct API to commit offsets to `markerTopic`
     * @param msgTimeoutMs Timeout, after which messages, if not processed, are redelivered.
     * @param useNowForRedeliverDespiteNoMarkerSeenForMs After what time "now" should be used to calculate redelivery
     * instead of maximum marker timestamp seen in a partition
     * @param deadLetterTopic Name of the Kafka topic containing all undelivered messages.
     * @param redeliveryCountHeader Name of the redelivery count header.
     * @param maxRedeliveryCount Max number of message redeliveries.
     */
    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String markerConsumerGroupId,
            String markerConsumerOffsetGroupId, long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs,
            String deadLetterTopic, String redeliveryCountHeader, int maxRedeliveryCount) {

        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.markerConsumerGroupId = markerConsumerGroupId;
        this.markerConsumerOffsetGroupId = markerConsumerOffsetGroupId;
        this.msgTimeoutMs = msgTimeoutMs;
        this.useNowForRedeliverDespiteNoMarkerSeenForMs = useNowForRedeliverDespiteNoMarkerSeenForMs;
        this.deadLetterTopic = deadLetterTopic;
        this.redeliveryCountHeader = redeliveryCountHeader;
        this.maxRedeliveryCount = maxRedeliveryCount;
    }

    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String markerConsumerGroupId,
            String markerConsumerOffsetGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs) {

        this(msgTopic, markerTopic, msgConsumerGroupId, markerConsumerGroupId,
                markerConsumerOffsetGroupId, msgTimeoutMs, useNowForRedeliverDespiteNoMarkerSeenForMs,
                msgTopic + DEFAULT_DEAD_LETTER_TOPIC_SUFFIX, DEFAULT_REDELIVERY_COUNT_HEADER, DEFAULT_MAX_REDELIVERY_COUNT);
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

    public String getMarkerConsumerGroupId() {
        return markerConsumerGroupId;
    }

    public String getMarkerConsumerOffsetGroupId() {
        return markerConsumerOffsetGroupId;
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
}
