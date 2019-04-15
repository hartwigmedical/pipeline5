package com.hartwig.pipeline.execution.vm;

import org.apache.commons.lang.Validate;

import static java.lang.String.format;

public class GoogleStorage {

    private String bucketName;
    public GoogleStorage(String bucketName) {
        Validate.notNull(bucketName, "Bucket name must not be null");
        Validate.notEmpty(bucketName.trim(), "Bucket name must not be empty");
        this.bucketName = bucketName;
    }

    public String create() {
        return format("gsutil mb -l europe-west4 gs://%s", bucketName);
    }

    public String copyToLocal(String remote, String localPath) {
        return format("gsutil -q cp gs://%s/%s %s", bucketName, remote, localPath);
    }

    public String copyFromLocal(String local, String remote) {
        return format("gsutil -q cp %s gs://%s/%s", local, bucketName, remote);
    }
}