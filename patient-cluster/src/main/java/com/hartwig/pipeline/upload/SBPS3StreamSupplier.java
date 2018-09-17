package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.InputStream;
import java.util.function.Function;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class SBPS3StreamSupplier implements Function<File, InputStream> {

    private final AmazonS3 s3client;

    private SBPS3StreamSupplier(final AmazonS3 s3client) {
        this.s3client = s3client;
    }

    @Override
    public InputStream apply(final File file) {
        String bucket = file.getParent().replaceAll("/", "");
        return s3client.getObject(bucket, file.getPath()).getObjectContent();
    }

    public static Function<File, InputStream> newInstance(String endpointUrl) {
        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
        AmazonS3 s3Client =
                clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, clientBuilder.getRegion()))
                        .build();
        return new SBPS3StreamSupplier(s3Client);
    }
}
