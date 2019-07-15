package com.hartwig.pipeline.transfer;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.S3;

public class SbpFileTransferProvider {
    private final Arguments arguments;
    private final Storage storage;

    private SbpFileTransferProvider(Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static SbpFileTransferProvider from(final Arguments arguments, final Storage storage) {
        return new SbpFileTransferProvider(arguments, storage);
    }

    public SbpFileTransfer get() {
        CloudCopy cloudCopy = new RCloneCloudCopy(arguments.rclonePath(),
                arguments.rcloneGcpRemote(),
                arguments.rcloneS3RemoteUpload(),
                ProcessBuilder::new);

        SbpS3 sbpS3 = new SbpS3(S3.newClient(arguments.sbpS3Url()), System.getenv());
        SbpRestApi sbpRestApi = SbpRestApi.newInstance(arguments);

        try {
            Bucket sourceBucket = storage.get(arguments.patientReportBucket());
            return new SbpFileTransfer(cloudCopy, sbpS3, sbpRestApi, sourceBucket, ContentTypeCorrection.get(), arguments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
