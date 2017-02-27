package com.softwaremill.kmq;

/**
 * Configuration for the Kafka-based MQ.
 */
public class KmqConfig {
    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryAppId;
    private final long msgTimeout;
    private final String startedMarkersStoreName;

    /**
     * @param msgTopic Name of the Kafka topic containing the messages.
     * @param markerTopic Name of the Kafka topic containing the markers.
     * @param msgConsumerGroupId Consumer group id for reading messages from `msgTopic`.
     * @param redeliveryAppId Kafka-streams app id for the redelivery component.
     * @param msgTimeout Timeout, after which messages, if not processed, are redelivered.
     * @param startedMarkersStoreName Name of the Kafka-streams store for non-processed message markers.
     */
    public KmqConfig(String msgTopic, String markerTopic, String msgConsumerGroupId, String redeliveryAppId, long msgTimeout, String startedMarkersStoreName) {
        this.msgTopic = msgTopic;
        this.markerTopic = markerTopic;
        this.msgConsumerGroupId = msgConsumerGroupId;
        this.redeliveryAppId = redeliveryAppId;
        this.msgTimeout = msgTimeout;
        this.startedMarkersStoreName = startedMarkersStoreName;
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

    public String getRedeliveryAppId() {
        return redeliveryAppId;
    }

    public long getMsgTimeout() {
        return msgTimeout;
    }

    public String getStartedMarkersStoreName() {
        return startedMarkersStoreName;
    }
}
