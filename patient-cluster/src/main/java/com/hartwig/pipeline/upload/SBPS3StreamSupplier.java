package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.InputStream;
import java.util.function.Function;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class SBPS3StreamSupplier implements Function<File, InputStream> {

    private static final String BUCKET = "hmf-fastq-storage";
    private final AmazonS3 s3client;

    private SBPS3StreamSupplier(final AmazonS3 s3client) {
        this.s3client = s3client;
    }

    @Override
    public InputStream apply(final File file) {
        return s3client.getObject(BUCKET, file.getPath()).getObjectContent();
    }

    public static Function<File, InputStream> newInstance() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        return new SBPS3StreamSupplier(s3Client);
    }
}
