package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.alignment.Aligner;

import org.junit.Before;
import org.junit.Test;

public class GoogleStorageStatusCheckTest {

    private static final NamespacedResults RESULTS_DIRECTORY = NamespacedResults.of(Aligner.RESULTS_NAMESPACE);
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
        statusCheck = new GoogleStorageStatusCheck(RESULTS_DIRECTORY);
    }

    @Test
    public void findsSuccessStatusInRuntimeGoogleStorageBucket() {
        when(bucket.get(RESULTS_DIRECTORY.path("_SUCCESS"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.SUCCESS);
    }

    @Test
    public void findsFailureStatusInRuntimeGoogleStorageBucket() {
        when(bucket.get(RESULTS_DIRECTORY.path("_FAILURE"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.FAILED);
    }

    @Test
    public void returnsUnknownWhenNoStatusFileFoundInBucket() {
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.UNKNOWN);
    }
}