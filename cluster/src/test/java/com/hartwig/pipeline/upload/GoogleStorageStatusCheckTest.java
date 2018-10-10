package com.hartwig.pipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class GoogleStorageStatusCheckTest {

    private RuntimeBucket runtime;
    private Bucket bucket;
    private Blob blob;
    private StatusCheck statusCheck;

    @Before
    public void setUp() throws Exception {
        runtime = mock(RuntimeBucket.class);
        bucket = mock(Bucket.class);
        blob = mock(Blob.class);
        when(runtime.bucket()).thenReturn(bucket);
        when(blob.getContent()).thenReturn("reason".getBytes());
        statusCheck = new GoogleStorageStatusCheck();
    }

    @Test
    public void findsSuccessStatusInRuntimeGoogleStorageBucket() {
        when(bucket.get("results/_SUCCESS")).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.SUCCESS);
    }

    @Test
    public void findsFailureStatusInRuntimeGoogleStorageBucket() {
        when(bucket.get("results/_FAILURE")).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.FAILED);
    }

    @Test
    public void returnsUnknownWhenNoStatusFileFoundInBucket() {
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.UNKNOWN);
    }
}