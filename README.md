# Kafka Message Queue

Using `kmq` you can acknowledge processing of individual messages in Kafka, and have unacknowledged messages 
re-delivered after a timeout. 

This is in contrast to the usual Kafka messageOffset-committing mechanism, using which you can acknowledge all messages
up to a given messageOffset only. 

If you are familiar with [Amazon SQS](https://aws.amazon.com/sqs/), `kmq` implements a similar message processing
model.

# How does this work?

For a more in-depth overview see the blog: [Using Kafka as a message queue](https://softwaremill.com/using-kafka-as-a-message-queue/)

The acknowledgment mechanism uses a `marker` topic, which should have the same number of partitions as the "main"
data topic (called the `queue` topic), and is used to track which messages have been processed, with start/end 
markers that should be written to the topic for every message.

# Using kmq

An application using `kmq` should consist of the following components:

* a number of `RedeliveryTracker`s. This components consumes the `marker` topic and redelivers messages if appropriate. 
Multiple copies should be started in a cluster for fail-over. Uses Kafka Streams and automatic partition assignment.
* components which send data to the `queue` topic to be processed
* queue clients, either custom or using the `KmqClient`

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

# Project status

No releases (yet?). The code is of proof-of-concept quality.