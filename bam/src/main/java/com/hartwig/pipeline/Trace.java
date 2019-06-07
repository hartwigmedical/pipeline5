package com.hartwig.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Trace {

    private static final String TEMPLATE = "{} : {}";

    private final Logger logger;
    private final String messagePrefix;
    private long startTimeMillis;
    private long executionTime;

    private Trace(final Logger logger, final String messagePrefix) {
        this.logger = logger;
        this.messagePrefix = messagePrefix;
    }

    public static Trace of(Class clazz, String messagePrefix) {
        return new Trace(LoggerFactory.getLogger(clazz), messagePrefix);
    }

    public Trace start() {
        logger.debug(TEMPLATE, messagePrefix, "Started");
        startTimeMillis = System.currentTimeMillis();
        return this;
    }

    void finish() {
        logger.debug(TEMPLATE, messagePrefix, "Completed");
        executionTime = System.currentTimeMillis() - startTimeMillis;
    }

    long getExecutionTime() {
        return executionTime;
    }
}
