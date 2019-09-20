package com.hartwig.pipeline.testsupport;

import static java.lang.String.format;

public class BucketInputOutput {
    private final String bucket;

    public BucketInputOutput(String bucket) {

        this.bucket = bucket;
    }

    public String push(String destination) {
        return format("gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r %s/ gs://%s/%s",
                CommonTestEntities.OUT_DIR, bucket, destination);
    }

    public String pull(String source) {
        return format("gsutil -qm cp -n gs://%s/%s %s/", bucket, source, CommonTestEntities.IN_DIR);
    }

    public String pull(String source, String destination) {
        return format("gsutil -qm cp -n gs://%s/%s %s/%s", bucket, source, CommonTestEntities.IN_DIR, destination);
    }

    public String resource(String source) {
        return format("gsutil -qm cp gs://%s/%s /data/resources", bucket, source);
    }
}
