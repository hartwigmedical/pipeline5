package com.hartwig.pipeline.failsafe;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import com.google.api.gax.rpc.InvalidArgumentException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.RetryPolicy;

public class DefaultBackoffPolicy<R> extends RetryPolicy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackoffPolicy.class);

    DefaultBackoffPolicy(final int delay, final long maxDelay, final String taskName) {
        withBackoff(delay, maxDelay, ChronoUnit.SECONDS);
        withMaxRetries(-1);
        abortOn(InvalidArgumentException.class);
        onAbort(e -> LOGGER.error("Unable to submit operation", e.getFailure()));
        handle(Exception.class);
        onFailedAttempt(rExecutionAttemptedEvent ->
        {
            // we need to keep tracing the cause to print out the real failure reason
            Throwable lastFailure = rExecutionAttemptedEvent.getLastFailure();
            while(lastFailure != null)
            {
                LOGGER.warn("[{}] failed: {}", taskName, lastFailure.getMessage());
                lastFailure = lastFailure.getCause();
            }
        });
    }

    public static <R> DefaultBackoffPolicy<R> of(final String taskName) {
        return new DefaultBackoffPolicy<>(1, TimeUnit.MINUTES.toSeconds(5), taskName);
    }
}