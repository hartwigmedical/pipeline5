package com.hartwig.pipeline.upload;

import com.hartwig.pipeline.bootstrap.RuntimeBucket;

public interface StatusCheck {

    enum Status {
        SUCCESS,
        FAILED,
        UNKNOWN
    }

    Status check(RuntimeBucket bucket);
}
