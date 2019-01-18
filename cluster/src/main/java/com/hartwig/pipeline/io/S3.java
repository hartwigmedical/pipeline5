package com.hartwig.pipeline.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3 {

    public static AmazonS3 newClient(String endpointUrl) {
        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();
       /* if (!endpointUrl.isEmpty()) {
            clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, clientBuilder.getRegion()));
        }*/
        return clientBuilder.build();
    }
}
