package com.hartwig.pipeline.failsafe;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.RetryPolicy;

public class DefaultBackoffPolicy<R> extends RetryPolicy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackoffPolicy.class);

    DefaultBackoffPolicy(final String taskName, final int delay, final long maxDelay) {
        withBackoff(delay, maxDelay, ChronoUnit.SECONDS);
        withMaxRetries(-1);
        handle(Exception.class);
        onFailedAttempt(rExecutionAttemptedEvent -> LOGGER.warn("Unable to execute action [{}] after [{}], trying again...",
                taskName,
                rExecutionAttemptedEvent.getElapsedTime().toSeconds()));
    }

    public static <R> DefaultBackoffPolicy<R> of(final String taskName) {
        return new DefaultBackoffPolicy<>(taskName, 1, TimeUnit.MINUTES.toSeconds(5));
    }
}
