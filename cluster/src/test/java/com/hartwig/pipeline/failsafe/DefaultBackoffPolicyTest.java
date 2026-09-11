package com.hartwig.pipeline.failsafe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedRunnable;

public class DefaultBackoffPolicyTest {

    private static final int EXPECTED_RETRIES = 3;
    private static final int UNLIMITED_RETRIES = -1;
    private static final String FAILURE = "task failed";

    @Test
    public void backsOffUntilMaxIsReached() {
        ThrowExceptionThrice thrower = new ThrowExceptionThrice();
        DefaultBackoffPolicy<Object> victim = new DefaultBackoffPolicy<>(1, 3, "task name", UNLIMITED_RETRIES);
        Failsafe.with(victim).run(thrower);
        assertThat(thrower.retries).isEqualTo(EXPECTED_RETRIES);
    }

    @Test
    public void stopsRetryingOnceBoundIsReached() {
        ThrowExceptionAlways thrower = new ThrowExceptionAlways();
        DefaultBackoffPolicy<Object> victim = new DefaultBackoffPolicy<>(1, 3, "task name", 1);
        assertThatThrownBy(() -> Failsafe.with(victim).run(thrower)).hasMessageContaining(FAILURE);
        assertThat(thrower.attempts).isEqualTo(2);
    }

    static class ThrowExceptionThrice implements CheckedRunnable{
        int retries;
        @Override
        public void run() {
            if (retries < EXPECTED_RETRIES){
                retries++;
                throw new RuntimeException();
            }
        }
    }

    static class ThrowExceptionAlways implements CheckedRunnable {
        int attempts;
        @Override
        public void run() {
            attempts++;
            throw new RuntimeException(FAILURE);
        }
    }

}
