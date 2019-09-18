package com.hartwig.pipeline.execution.vm;

import java.time.Duration;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.storage.RuntimeBucket;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class BucketCompletionWatcher {
    enum State {
        SUCCESS,
        FAILURE,
        STILL_WAITING
    }

    State waitForCompletion(RuntimeBucket bucket, VirtualMachineJobDefinition jobDefinition) {
        return Failsafe.with(new RetryPolicy<>().withMaxRetries(-1).withDelay(Duration.ofSeconds(5)).handleResult(null)).get(() -> {
            State currentState = currentState(bucket, jobDefinition);
            if (currentState.equals(State.STILL_WAITING)) {
                return null;
            } else {
                return currentState;
            }
        });
    }

    State currentState(RuntimeBucket bucket, VirtualMachineJobDefinition jobDefinition) {
        if (bucketContainsFile(bucket, jobDefinition.startupCommand().failureFlag())) {
            return State.FAILURE;
        } else if (bucketContainsFile(bucket, jobDefinition.startupCommand().successFlag())) {
            return State.SUCCESS;
        }
        return State.STILL_WAITING;
    }

    private boolean bucketContainsFile(RuntimeBucket bucket, String filename) {
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
