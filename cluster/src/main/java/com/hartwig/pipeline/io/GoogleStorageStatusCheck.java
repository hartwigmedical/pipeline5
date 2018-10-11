package com.hartwig.pipeline.io;

import com.google.cloud.storage.Blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageStatusCheck implements StatusCheck {

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageStatusCheck.class);

    private static final String SUCCESS_PATH = "results/_SUCCESS";
    private static final String FAILURE_PATH = "results/_FAILURE";

    @Override
    public Status check(final RuntimeBucket bucket) {
        Blob blob = bucket.bucket().get(FAILURE_PATH);
        if (blob != null) {
            LOGGER.error("Pipeline run for [{}] was marked as failed with reason [{}]", bucket.getName(), new String(blob.getContent()));
            return Status.FAILED;
        }
        blob = bucket.bucket().get(SUCCESS_PATH);
        if (blob != null) {
            LOGGER.info("Pipeline run for [{}] was marked as successful", bucket.getName());
            return Status.SUCCESS;
        }
        LOGGER.warn("Pipeline run for [{}] had no status. Check job logs for more information", bucket.getName());
        return Status.UNKNOWN;
    }
}
