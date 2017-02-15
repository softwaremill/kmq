package com.softwaremill.kmq.example;

import com.softwaremill.kmq.RedeliveryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UncaughtExceptionHandling {
    private final static Logger LOG = LoggerFactory.getLogger(RedeliveryExecutor.class);

    public static void setup() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in thread " + t, e));
    }
}
