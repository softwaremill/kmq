# Kafka Message Queue

[![Join the chat at https://gitter.im/softwaremill/kmq](https://badges.gitter.im/softwaremill/kmq.svg)](https://gitter.im/softwaremill/kmq?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.softwaremill.kmq/core_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.softwaremill.kmq/core_2.12)

Using `kmq` you can acknowledge processing of individual messages in Kafka, and have unacknowledged messages 
re-delivered after a timeout. 

This is in contrast to the usual Kafka offset-committing mechanism, using which you can acknowledge all messages
up to a given offset only. 

If you are familiar with [Amazon SQS](https://aws.amazon.com/sqs/), `kmq` implements a similar message processing
model.

# How does this work?

For a more in-depth overview see the blog: [Using Kafka as a message queue](https://softwaremill.com/using-kafka-as-a-message-queue/),
and for performance benchmarks: [Kafka with selective acknowledgments (kmq) performance & latency benchmark](https://softwaremill.com/kafka-with-selective-acknowledgments-performance/)

The acknowledgment mechanism uses a `marker` topic, which should have the same number of partitions as the "main"
data topic (called the `queue` topic). The marker topic is used to track which messages have been processed, by 
writing start/end  markers for every message.

![message flow diagram](https://softwaremill.com/images/uploads/2017/02/kmq.93f842cf.png)

# Using kmq

An application using `kmq` should consist of the following components:

* a number of `RedeliveryTracker`s. This components consumes the `marker` topic and redelivers messages if appropriate. 
Multiple copies should be started in a cluster for fail-over. Uses automatic partition assignment.
* components which send data to the `queue` topic to be processed
* queue clients, either custom or using the `KmqClient`     

# Maven/SBT dependency

SBT:

    "com.softwaremill.kmq" %% "core" % "0.2"

Maven:

    <dependency>
        <groupId>com.softwaremill.kmq</groupId>
        <artifactId>core_2.12</artifactId>
        <version>0.2</version>
    </dependency>

(Use `core_2.11` if you are using other components depending on Scala 2.11.)

# Client flow

The flow of processing a message is as follow:

1. read messages from the `queue` topic, in batches
2. write a `start` marker to the `markers` topic for each message, wait until the markers are written
3. commit the biggest message offset to the `queue` topic
4. process messages
5. for each message, write an `end` marker. No need to wait until the markers are written.

This ensures at-least-once processing of each message. Note that the acknowledgment of each message (writing the 
`end` marker) can be done for each message separately, out-of-order, from a different thread, server or application.

# Example code

There are three example applications:

* `example-java/embedded`: a single java application that starts all three components (sender, client, redelivery tracker)
* `example-java/standalone`: three separate runnable classes to start the different components
* `example-scala`: an implementation of the client using [reactive-kafka](https://github.com/akka/reactive-kafka)

# Time & timestamps

How time is handled is crucial for message redelivery, as messages are redelivered after a given amount of time passes
since the `start` marker was sent.

To track what was sent when, `kmq` uses Kafka's message timestamp. By default this is messages create time
(`message.timestamp.type=CreateTime`), but for the `markers` topic, it is advisable to switch this to `LogAppendTime`.
That way, the timestamps more closely reflect when the markers are really written to the log, and are guaranteed to be
monotonic in each partition (which is important for redelivery - see below).

To calculate which messages should be redelivered, we need to know the value of "now", to check which `start` markers
have been sent later than the configured timeout. When a marker has been received from a partition recently, the
maximum such timestamp is used as the value of "now" - as it indicates exactly how far we are in processing the
partition. What "recently" means depends on the `useNowForRedeliverDespiteNoMarkerSeenForMs` config setting. Otherwise,
the current system time is used, as we assume that all markers from the partition have been processed.

# Project status

## Version 0.2 (19 Jun 2017)

* redelivery component optimizations
* bug fixes

## Version 0.1 (24 Apr 2017)

* initial release