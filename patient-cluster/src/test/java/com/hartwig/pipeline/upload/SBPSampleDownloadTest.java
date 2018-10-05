package com.hartwig.pipeline.upload;

import static org.mockito.Mockito.mock;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.junit.Test;

public class SBPSampleDownloadTest {

    @Test
    public void setsAclsWhenGivenAsEnvironmentVariables() throws Exception {
        new ProcessBuilder("sh", "export", "READER_ACL_IDS=test1").start().waitFor();
        SBPSampleDownload victim = new SBPSampleDownload(AmazonS3ClientBuilder.defaultClient(), null, 1, (sample, runtimeBucket) -> {

        });
        victim.run(Sample.builder("test", "CPCT12345678R").barcode("HJJLGCCXX").build(), mock(RuntimeBucket.class));
    }

}