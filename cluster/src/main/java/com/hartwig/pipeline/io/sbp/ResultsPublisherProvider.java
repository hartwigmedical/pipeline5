package com.hartwig.pipeline.io.sbp;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.CloudCopy;
import com.hartwig.pipeline.io.RCloneCloudCopy;
import com.hartwig.pipeline.io.S3;

public class ResultsPublisherProvider {
    private final Arguments arguments;
    private final Storage storage;

    private ResultsPublisherProvider(Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static ResultsPublisherProvider from(final Arguments arguments, final Storage storage) {
        return new ResultsPublisherProvider(arguments, storage);
    }

    public ResultsPublisher get() {
        CloudCopy cloudCopy = new RCloneCloudCopy(arguments.rclonePath(),
                arguments.rcloneGcpRemote(),
                arguments.rcloneS3RemoteUpload(),
                ProcessBuilder::new);

        SbpS3 sbpS3 = new SbpS3(S3.newClient(arguments.sbpS3Url()), System.getenv());
        SbpRestApi sbpRestApi = SbpRestApi.newInstance(arguments);

        try {
            Bucket sourceBucket = storage.get(arguments.patientReportBucket());
            return new ResultsPublisher(cloudCopy, sbpS3, sbpRestApi, sourceBucket);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
