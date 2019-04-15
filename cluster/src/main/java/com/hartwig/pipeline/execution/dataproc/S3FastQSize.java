package com.hartwig.pipeline.execution.dataproc;

import java.io.File;
import java.util.function.ToDoubleFunction;

import com.amazonaws.services.s3.AmazonS3;

public class S3FastQSize implements ToDoubleFunction<String> {
    private final AmazonS3 s3;

    public S3FastQSize(final AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public double applyAsDouble(final String filename) {
        File file = new File(filename);
        String bucket = file.getParent().replaceAll("/", "");
        return s3.getObject(bucket, file.getName()).getObjectMetadata().getContentLength() / 1e9;
    }
}
