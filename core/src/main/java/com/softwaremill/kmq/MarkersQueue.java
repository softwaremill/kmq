package com.softwaremill.kmq;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;

/**
 * Thread-safe if `isEnded` is thread-safe.
 */
class MarkersQueue {
    private final Function<MarkerKey, Boolean> isEnded;
    private final BlockingQueue<Marker> markersQueue;
    private final Clock clock;
    private final long messageTimeout;

    MarkersQueue(Function<MarkerKey, Boolean> isEnded, Clock clock, long messageTimeout) {
        this.isEnded = isEnded;
        this.clock = clock;
        this.messageTimeout = messageTimeout;
        markersQueue = new PriorityBlockingQueue<>(); // TODO: bounds
    }

    void offer(MarkerKey k, MarkerValue v) {
        markersQueue.offer(new Marker(k, v));
    }

    void removeEndedMarkers() {
        while (isHeadEnded()) {
            markersQueue.poll();
        }
    }

    private boolean isHeadEnded() {
        Marker head = markersQueue.peek();
        return head != null && isEnded.apply(head.key);
    }

    List<Marker> markersToRedeliver() {
        List<Marker> toRedeliver = new ArrayList<>();
        while (shouldRedeliverQueueHead()) {
            Marker queueHead = markersQueue.poll();
            // the first marker, if any, is not ended for sure (b/c of the cleanup that's done on every punctuate),
            // but subsequent markers don't have to be.
            if (!isEnded.apply(queueHead.key)) {
                toRedeliver.add(queueHead);
            }
        }

        return toRedeliver;
    }

    private boolean shouldRedeliverQueueHead() {
        if (!markersQueue.isEmpty()) {
            long queueHeadTimestamp = markersQueue.peek().value.getProcessingTimestamp();
            return clock.millis() - queueHeadTimestamp >= messageTimeout;
        } else return false;
    }

    static class Marker implements Comparable<Marker> {
        final MarkerKey key;
        final MarkerValue value;

        Marker(MarkerKey key, MarkerValue value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Marker o) {
            long diff = value.getProcessingTimestamp() - o.value.getProcessingTimestamp();
            return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
        }
    }
}
