package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;

import org.junit.Before;
import org.junit.Test;

public class GoogleStorageStatusCheckTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private RuntimeBucket runtime;
    private Blob blob;
    private StatusCheck statusCheck;

    @Before
    public void setUp() throws Exception {
        runtime = mock(RuntimeBucket.class);
        blob = mock(Blob.class);
        when(blob.getContent()).thenReturn("reason".getBytes());
        statusCheck = new GoogleStorageStatusCheck(RESULTS_DIRECTORY);
    }

    @Test
    public void findsSuccessStatusInRuntimeGoogleStorageBucket() {
        when(runtime.get(RESULTS_DIRECTORY.path("_SUCCESS"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.SUCCESS);
    }

    @Test
    public void findsFailureStatusInRuntimeGoogleStorageBucket() {
        when(runtime.get(RESULTS_DIRECTORY.path("_FAILURE"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.FAILED);
    }

    @Test
    public void returnsUnknownWhenNoStatusFileFoundInBucket() {
        assertThat(statusCheck.check(runtime)).isEqualTo(StatusCheck.Status.UNKNOWN);
    }
}