package com.softwaremill.kmq;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private static final int MAX_REDELIVERY_COUNT = 3;
    private static final String REDELIVERY_COUNT_HEADER = "kmq-redelivery-count";

    private final String msgTopic;
    private final String markerTopic;
    private final String deadLetterTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryConsumerGroupId;
    private final long msgTimeoutMs;
    private final long useNowForRedeliverDespiteNoMarkerSeenForMs;
    private final String redeliveryCountHeader;
    private final int maxRedeliveryCount;

    /**
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param deadLetterTopic Name of the Kafka topic containing all undelivered messages.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param redeliveryConsumerGroupId Consumer group id for reading messages from `markerTopic`.
     * @param msgTimeoutMs Timeout, after which messages, if not processed, are redelivered.
     * @param useNowForRedeliverDespiteNoMarkerSeenForMs After what time "now" should be use to calculate redelivery
     *                                                   instead of maximum marker timestamp seen in a partition
     * @param redeliveryCountHeader Name of the redelivery count header.
     * @param maxRedeliveryCount Max number of message redeliveries.
     */
    public KmqConfig(
            String msgTopic, String markerTopic, String deadLetterTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs,
            String redeliveryCountHeader, int maxRedeliveryCount) {

        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.redeliveryConsumerGroupId = redeliveryConsumerGroupId;
        this.msgTimeoutMs = msgTimeoutMs;
        this.useNowForRedeliverDespiteNoMarkerSeenForMs = useNowForRedeliverDespiteNoMarkerSeenForMs;
        this.redeliveryCountHeader = redeliveryCountHeader;
        this.maxRedeliveryCount = maxRedeliveryCount;
    }

    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs) {

        this(msgTopic, markerTopic, msgTopic + "__undelivered", msgConsumerGroupId, redeliveryConsumerGroupId,
                msgTimeoutMs, useNowForRedeliverDespiteNoMarkerSeenForMs,
                REDELIVERY_COUNT_HEADER, MAX_REDELIVERY_COUNT);
    }

    public String getMsgTopic() {
        return msgTopic;
    }

    public String getMarkerTopic() {
        return markerTopic;
    }

    public String getDeadLetterTopic() {
        return deadLetterTopic;
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

    public String getRedeliveryCountHeader() {
        return redeliveryCountHeader;
    }

    public int getMaxRedeliveryCount() {
        return maxRedeliveryCount;
    }
}
