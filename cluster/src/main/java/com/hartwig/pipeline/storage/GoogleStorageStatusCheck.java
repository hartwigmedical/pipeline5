package com.hartwig.pipeline.storage;

import static java.lang.String.format;

import com.google.cloud.storage.Blob;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageStatusCheck implements StatusCheck {

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageStatusCheck.class);
    private final ResultsDirectory resultsDirectory;

    public GoogleStorageStatusCheck(final ResultsDirectory resultsDirectory) {
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public Status check(final RuntimeBucket bucket, final String jobName) {
        Blob blob = bucket.get(resultsDirectory.path(format("/%s_FAILURE", jobName)));
        if (blob != null) {
            return Status.FAILED;
        }
        blob = bucket.get(resultsDirectory.path(format("/%s_SUCCESS", jobName)));
        if (blob != null) {
            LOGGER.debug("Pipeline run for [{}] was marked as successful", bucket.name());
            return Status.SUCCESS;
        }
        return Status.UNKNOWN;
    }
}
