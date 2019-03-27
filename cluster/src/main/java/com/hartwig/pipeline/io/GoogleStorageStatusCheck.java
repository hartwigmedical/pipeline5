package com.hartwig.pipeline.io;

import com.google.cloud.storage.Blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageStatusCheck implements StatusCheck {

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageStatusCheck.class);

    private static final String SUCCESS_PATH = "_SUCCESS";
    private static final String FAILURE_PATH = "_FAILURE";

    private final ResultsDirectory resultsDirectory;

    public GoogleStorageStatusCheck(final ResultsDirectory resultsDirectory) {
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public Status check(final RuntimeBucket bucket) {
        Blob blob = bucket.bucket().get(resultsDirectory.path(FAILURE_PATH));
        if (blob != null) {
            LOGGER.error("Pipeline run for [{}] was marked as failed with reason [{}]", bucket.name(), new String(blob.getContent()));
            return Status.FAILED;
        }
        blob = bucket.bucket().get(resultsDirectory.path(SUCCESS_PATH));
        if (blob != null) {
            LOGGER.info("Pipeline run for [{}] was marked as successful", bucket.name());
            return Status.SUCCESS;
        }
        LOGGER.warn("Pipeline run for [{}] had no status. Check job logs for more information", bucket.name());
        return Status.UNKNOWN;
    }
}
