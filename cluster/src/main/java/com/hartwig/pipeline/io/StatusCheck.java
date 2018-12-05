package com.hartwig.pipeline.io;

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
