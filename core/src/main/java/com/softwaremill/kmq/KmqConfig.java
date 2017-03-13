package com.softwaremill.kmq;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryConsumerGroupId;
    private final long msgTimeoutMs;
    private final long useNowForRedeliverDespiteNoMarkerSeenForMs;

    /**
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param redeliveryConsumerGroupId Consumer grou pid for reading messages from `markerTopic`.
     * @param msgTimeoutMs Timeout, after which messages, if not processed, are redelivered.
     * @param useNowForRedeliverDespiteNoMarkerSeenForMs After what time "now" should be use to calculate redelivery
     *                                                   instead of maximum marker timestamp seen in a partition
     */
    public KmqConfig(
            String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryConsumerGroupId,
            long msgTimeoutMs, long useNowForRedeliverDespiteNoMarkerSeenForMs) {

        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.redeliveryConsumerGroupId = redeliveryConsumerGroupId;
        this.msgTimeoutMs = msgTimeoutMs;
        this.useNowForRedeliverDespiteNoMarkerSeenForMs = useNowForRedeliverDespiteNoMarkerSeenForMs;
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
}
