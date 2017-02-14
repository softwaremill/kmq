package com.softwaremill.kmq;

public class KmqConfig {
    private final String msgTopic;
    private final String markerTopic;
    private final String msgConsumerGroupId;
    private final String redeliveryAppId;
    private final long msgTimeout;
    private final String startedMarkersStoreName;

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
