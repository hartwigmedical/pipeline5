package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.InputStream;
import java.util.function.Function;

import com.amazonaws.services.s3.AmazonS3;

public class SBPS3InputStreamProvider implements Function<File, InputStream> {

    private final AmazonS3 s3client;

    private SBPS3InputStreamProvider(final AmazonS3 s3client) {
        this.s3client = s3client;
    }

    @Override
    public InputStream apply(final File file) {
        String bucket = file.getParent().replaceAll("/", "");
        return s3client.getObject(bucket, file.getName()).getObjectContent();
    }

    public static Function<File, InputStream> newInstance(AmazonS3 s3) {
        return new SBPS3InputStreamProvider(s3);
    }
}
