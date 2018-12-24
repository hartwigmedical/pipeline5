package com.hartwig.pipeline.io.sbp;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.bootstrap.JobResult;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class SBPS3BamDownloadTest {

    @Test
    public void streamFromGoogleToS3() {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        SBPS3BamDownload victim = new SBPS3BamDownload(s3, ResultsDirectory.defaultDirectory());

        Storage storage = StorageOptions.getDefaultInstance().getService();

        RuntimeBucket bucket = RuntimeBucket.from(storage, "COLO829R", Arguments.defaultsBuilder().runId("20181222085059259").build());
        Sample sample = Sample.builder("", "COLO829R").barcode("FR1234").build();

        victim.run(sample, bucket, JobResult.SUCCESS);

    }
}