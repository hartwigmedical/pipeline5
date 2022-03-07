package com.hartwig.pipeline.smoke;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

public class Parallelized extends BlockJUnit4ClassRunner {

    private static class ThreadPoolScheduler implements RunnerScheduler {
        private final ExecutorService executor;

        public ThreadPoolScheduler() {
            executor = Executors.newCachedThreadPool();
        }

        @Override
        public void finished() {
            executor.shutdown();
            try {
                assertThat(executor.awaitTermination(30, TimeUnit.MINUTES)).isTrue();
            } catch (InterruptedException exc) {
                throw new RuntimeException(exc);
            }
        }

        @Override
        public void schedule(final Runnable childStatement) {
            executor.submit(childStatement);
        }
    }

    public Parallelized(final Class klass) throws Throwable {
        super(klass);
        setScheduler(new ThreadPoolScheduler());
    }
}
