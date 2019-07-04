package com.hartwig.pipeline.io.sbp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;

import org.junit.Before;
import org.junit.Test;

public class SbpS3Test {
    private String bucket = "some_bucket";
    private AmazonS3 s3;
    private SbpS3 sbp;

    @Before
    public void setup() {
        s3 = mock(AmazonS3.class);
        sbp = new SbpS3(s3);
    }

    @Test
    public void shouldThrowIfBucketDoesNotExist() {
        when(s3.doesBucketExistV2(bucket)).thenReturn(false);
        try {
            sbp.ensureBucketExists(bucket);
        } catch (IllegalStateException ise) {
            // what we want
        }
    }

    @Test
    public void shouldNotThrowIfBucketExists() {
        when(s3.doesBucketExistV2(bucket)).thenReturn(true);
        sbp.ensureBucketExists(bucket);
        verify(s3).doesBucketExistV2(bucket);
        verifyNoMoreInteractions(s3);
    }
}