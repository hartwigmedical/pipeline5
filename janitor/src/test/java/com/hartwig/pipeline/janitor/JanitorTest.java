package com.hartwig.pipeline.janitor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class JanitorTest {

    private Janitor victim;

    @Before
    public void setUp() throws Exception {
        victim = new Janitor(Executors.newSingleThreadScheduledExecutor());
    }

    @Test
    public void runsPeriodically() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        victim.register(arguments -> latch.countDown());
        victim.start(Arguments.defaults());
        assertThat(latch.await(10, TimeUnit.MILLISECONDS)).isTrue();
    }
}