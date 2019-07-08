package com.hartwig.pipeline.storage;

public interface StatusCheck {

    enum Status {
        SUCCESS,
        FAILED,
        UNKNOWN
    }

    Status check(RuntimeBucket bucket);

    static StatusCheck alwaysSuccess() {
        return bucket -> Status.SUCCESS;
    }
}
