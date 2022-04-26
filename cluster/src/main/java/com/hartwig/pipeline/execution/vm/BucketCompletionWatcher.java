package com.hartwig.pipeline.execution.vm;

import java.time.Duration;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.storage.RuntimeBucket;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

class BucketCompletionWatcher {
    enum State {
        SUCCESS,
        FAILURE,
        STILL_WAITING
    }

    State waitForCompletion(final RuntimeBucket bucket, final RuntimeFiles flags) {
        return Failsafe.with(new RetryPolicy<>().withMaxRetries(-1).withDelay(Duration.ofSeconds(5)).handleResult(null)).get(() -> {
            State currentState = currentState(bucket, flags);
            if (currentState.equals(State.STILL_WAITING)) {
                return null;
            } else {
                return currentState;
            }
        });
    }

    State currentState(final RuntimeBucket bucket, final RuntimeFiles flags) {
        if (bucketContainsFile(bucket, flags.failure())) {
            return State.FAILURE;
        } else if (bucketContainsFile(bucket, flags.success())) {
            return State.SUCCESS;
        }
        return State.STILL_WAITING;
    }

    private boolean bucketContainsFile(final RuntimeBucket bucket, final String filename) {
        List<Blob> objects = bucket.list();
        for (Blob blob : objects) {
            String name = blob.getName();
            if (name.equals(bucket.getNamespace() + "/" + filename)) {
                return true;
            }
        }
        return false;
    }
}
