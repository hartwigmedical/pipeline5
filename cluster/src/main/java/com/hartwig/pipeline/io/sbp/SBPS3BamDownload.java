package com.hartwig.pipeline.io.sbp;

import java.nio.channels.Channels;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.cloud.storage.Blob;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.JobResult;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.BamNames;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPS3BamDownload implements BamDownload {

    private final Logger LOGGER = LoggerFactory.getLogger(SBPS3BamDownload.class);

    private final TransferManager transferManager;
    private final ResultsDirectory resultsDirectory;

    SBPS3BamDownload(final TransferManager transferManager, final ResultsDirectory resultsDirectory) {
        this.transferManager = transferManager;
        this.resultsDirectory = resultsDirectory;
    }

    public static SBPS3BamDownload from(final AmazonS3 s3, final ResultsDirectory resultsDirectory, final int numThreads) {
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(s3)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(numThreads))
                .build();
        return new SBPS3BamDownload(transferManager, resultsDirectory);
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final JobResult result) {
        String[] path = SBPS3FileTarget.from(sample).replace("s3://", "").split("/", 2);
        String bucket = path[0];
        String bamKey = path[1];
        String baiKey = path[1] + ".bai";

        transfer(runtimeBucket, transferManager, BamNames.sorted(sample), bucket, bamKey);
        transfer(runtimeBucket, transferManager, BamNames.bai(sample), bucket, baiKey);
        transferManager.shutdownNow(false);
    }

    private void transfer(final RuntimeBucket runtimeBucket, final TransferManager transferManager, final String blobName,
            final String s3Bucket, final String s3Key) {
        Blob blob = runtimeBucket.bucket().get(resultsDirectory.path(blobName));

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(blob.getSize());
        PutObjectRequest request = new PutObjectRequest(s3Bucket, s3Key, Channels.newInputStream(blob.reader()), metadata);
        request.getRequestClientOptions().setReadLimit(Integer.MAX_VALUE);

        LOGGER.info("Downloading from [gs://{}/{}] and uploading it to SBP S3 at [s3://{}/{}]",
                runtimeBucket.bucket().getName(),
                blob.getName(),
                s3Bucket,
                s3Key);
        upload(transferManager, request);
    }

    private void upload(final TransferManager transferManager, final PutObjectRequest request) {
        Upload upload = transferManager.upload(request);
        try {
            upload.waitForCompletion();
            LOGGER.info("Transfer complete");
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the upload result. This is an unexpected error and likely caused by some external"
                    + "signal terminating the process");
            Thread.interrupted();
        }
    }
}
