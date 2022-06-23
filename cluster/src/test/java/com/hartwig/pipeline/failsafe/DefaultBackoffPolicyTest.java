package com.hartwig.pipeline.failsafe;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.function.CheckedRunnable;

public class DefaultBackoffPolicyTest {

    private static final int EXPECTED_RETRIES = 3;

    @Test
    public void backsOffUntilMaxIsReached() {
        ThrowExceptionThrice thrower = new ThrowExceptionThrice();
        DefaultBackoffPolicy<Object> victim = new DefaultBackoffPolicy<>(1, 3, "task name");
        Failsafe.with(victim).run(thrower);
        assertThat(thrower.retries).isEqualTo(EXPECTED_RETRIES);
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

}