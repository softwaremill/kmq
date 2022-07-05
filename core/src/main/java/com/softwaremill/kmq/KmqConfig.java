package com.softwaremill.kmq;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryConsumerGroupId;
    private final String redeliveryCountHeader;
    private final long msgTimeoutMs;
    private final long useNowForRedeliverDespiteNoMarkerSeenForMs;
    private final int maxRedeliveryCount;

    /**
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param redeliveryConsumerGroupId Consumer group id for reading messages from `markerTopic`.
     * @param redeliveryCountHeader Name of the redelivery count header.
     * @param msgTimeoutMs Timeout, after which messages, if not processed, are redelivered.
     * @param useNowForRedeliverDespiteNoMarkerSeenForMs After what time "now" should be use to calculate redelivery
     *                                                   instead of maximum marker timestamp seen in a partition
     * @param maxRedeliveryCount Max number of message redeliveries.
     */
    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId, String redeliveryCountHeader,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs, int maxRedeliveryCount) {

        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.redeliveryConsumerGroupId = redeliveryConsumerGroupId;
        this.redeliveryCountHeader = redeliveryCountHeader;
        this.msgTimeoutMs = msgTimeoutMs;
        this.useNowForRedeliverDespiteNoMarkerSeenForMs = useNowForRedeliverDespiteNoMarkerSeenForMs;
        this.maxRedeliveryCount = maxRedeliveryCount;
    }

    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs) {
        this(msgTopic, markerTopic, msgConsumerGroupId, redeliveryConsumerGroupId, "kmq-redelivery-count",
                msgTimeoutMs, useNowForRedeliverDespiteNoMarkerSeenForMs, 3);
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

    public String getRedeliveryCountHeader() {
        return redeliveryCountHeader;
    }

    public long getMsgTimeoutMs() {
        return msgTimeoutMs;
    }

    public long getUseNowForRedeliverDespiteNoMarkerSeenForMs() {
        return useNowForRedeliverDespiteNoMarkerSeenForMs;
    }

    public int getMaxRedeliveryCount() {
        return maxRedeliveryCount;
    }
}
