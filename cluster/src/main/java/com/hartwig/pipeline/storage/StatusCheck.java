package com.hartwig.pipeline.storage;

import com.hartwig.computeengine.storage.RuntimeBucket;

public interface StatusCheck {

    enum Status {
        SUCCESS,
        FAILED,
        UNKNOWN
    }

    Status check(RuntimeBucket bucket, String jobName);
}
