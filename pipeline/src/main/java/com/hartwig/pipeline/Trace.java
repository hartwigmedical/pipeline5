package com.hartwig.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Trace {

    private static final String TEMPLATE = "{} : {}";

    private final Logger logger;
    private final String messagePrefix;

    private Trace(final Logger logger, final String messagePrefix) {
        this.logger = logger;
        this.messagePrefix = messagePrefix;
    }

    public static Trace of(Class clazz, String messagePrefix) {
        return new Trace(LoggerFactory.getLogger(clazz), messagePrefix);
    }

    public void start() {
        logger.info(TEMPLATE, messagePrefix, "Started");
    }

    public void finish() {
        logger.info(TEMPLATE, messagePrefix, "Completed");
    }
}
