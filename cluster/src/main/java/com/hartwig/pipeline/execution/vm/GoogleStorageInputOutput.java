package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

public class GoogleStorageInputOutput {

    private final String bucketName;
    public GoogleStorageInputOutput(String bucketName) {
        this.bucketName = bucketName;
    }

    public String copyToLocal(String remote, String localPath) {
        return format("gsutil -qm cp gs://%s/%s %s", bucketName, remote, localPath);
    }

    public String copyFromLocal(String local, String remote) {
        return format("gsutil -qm cp %s gs://%s/%s", local, bucketName, remote);
    }
}