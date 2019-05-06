package com.hartwig.pipeline.io.sbp;

import java.nio.channels.Channels;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.cloud.storage.Blob;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.alignment.AlignmentOutputPaths;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SBPS3BamDownload implements BamDownload {

    private static final long ONE_HUNDRED_MB = 100 * 1024 * 1024L;
    private final Logger LOGGER = LoggerFactory.getLogger(SBPS3BamDownload.class);

    private final TransferManager transferManager;
    private final ResultsDirectory resultsDirectory;
    private final int maxAttempts;
    private final int retryDelay;

    SBPS3BamDownload(final TransferManager transferManager, final ResultsDirectory resultsDirectory, final int maxAttempts,
            final int retryDelay) {
        this.transferManager = transferManager;
        this.resultsDirectory = resultsDirectory;
        this.maxAttempts = maxAttempts;
        this.retryDelay = retryDelay;
    }

    public static SBPS3BamDownload from(final AmazonS3 s3, final ResultsDirectory resultsDirectory) {
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(s3)
                .withMultipartUploadThreshold(ONE_HUNDRED_MB)
                .withMinimumUploadPartSize(ONE_HUNDRED_MB)
                .build();
        return new SBPS3BamDownload(transferManager, resultsDirectory, 2, 600);
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final JobStatus result) {
        int attempts = 1;
        boolean success = false;
        while (attempts <= maxAttempts && !success) {
            try {
                String[] path = SBPS3FileTarget.from(sample).replace("s3://", "").split("/", 2);
                String bucket = path[0];
                String bamKey = path[1];
                String baiKey = path[1] + ".bai";
                String sorted = AlignmentOutputPaths.sorted(sample);
                transfer(runtimeBucket, transferManager, sorted, bucket, bamKey);
                transfer(runtimeBucket, transferManager, AlignmentOutputPaths.bai(sorted), bucket, baiKey);
                success = true;
            } catch (Exception e) {
                if (attempts <= maxAttempts) {
                    LOGGER.warn(String.format(
                            "Transfer failed on the following exception. This was attempt [%s] defaultDirectory [%s]. Retrying in [%s seconds]",
                            attempts,
                            maxAttempts,
                            retryDelay), e);
                    delay();
                } else {
                    LOGGER.error("Max attempts reached. Failing hard");
                    throw new RuntimeException(e);
                }
            } finally {
                attempts++;
            }
        }
        transferManager.shutdownNow(false);
    }

    private void delay() {
        try {
            Thread.sleep(1000 * retryDelay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void transfer(final RuntimeBucket runtimeBucket, final TransferManager transferManager, final String blobName,
            final String s3Bucket, final String s3Key) {
        Blob blob = runtimeBucket.get(resultsDirectory.path(blobName));

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(blob.getSize());
        PutObjectRequest request = new PutObjectRequest(s3Bucket, s3Key, Channels.newInputStream(blob.reader()), metadata);
        request.getRequestClientOptions().setReadLimit(Integer.MAX_VALUE);

        LOGGER.info("Downloading from [gs://{}/{}] and uploading it to SBP S3 at [s3://{}/{}]",
                runtimeBucket.name(),
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
