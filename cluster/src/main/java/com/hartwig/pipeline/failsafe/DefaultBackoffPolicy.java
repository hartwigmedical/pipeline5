package com.hartwig.pipeline.failsafe;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.RetryPolicy;

public class DefaultBackoffPolicy<R> extends RetryPolicy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackoffPolicy.class);

    private DefaultBackoffPolicy(final String taskName) {
        withBackoff(1, TimeUnit.MINUTES.toSeconds(5), ChronoUnit.SECONDS);
        handle(Exception.class);
        onFailedAttempt(rExecutionAttemptedEvent -> LOGGER.warn("Unable to execute action [{}] after [{}], trying again...",
                taskName,
                rExecutionAttemptedEvent.getElapsedTime()));
    }

    public static <R> DefaultBackoffPolicy<R> of(final String taskName) {
        return new DefaultBackoffPolicy<R>(taskName);
    }
}
