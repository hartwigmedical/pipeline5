package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.ResultsDirectory;

import org.junit.Before;
import org.junit.Test;

public class GoogleStorageStatusCheckTest {

    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private RuntimeBucket runtime;
    private Blob blob;
    private StatusCheck statusCheck;
    private String jobName;

    @Before
    public void setUp() throws Exception {
        runtime = mock(RuntimeBucket.class);
        blob = mock(Blob.class);
        jobName = "job_name";
        when(blob.getContent()).thenReturn("reason".getBytes());
        statusCheck = new GoogleStorageStatusCheck(RESULTS_DIRECTORY);
    }

    @Test
    public void findsSuccessStatusInRuntimeGoogleStorageBucket() {
        when(runtime.get(RESULTS_DIRECTORY.path("_SUCCESS"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime, jobName)).isEqualTo(StatusCheck.Status.SUCCESS);
    }

    @Test
    public void findsFailureStatusInRuntimeGoogleStorageBucket() {
        when(runtime.get(RESULTS_DIRECTORY.path("_FAILURE"))).thenReturn(blob);
        assertThat(statusCheck.check(runtime, jobName)).isEqualTo(StatusCheck.Status.FAILED);
    }

    @Test
    public void returnsUnknownWhenNoStatusFileFoundInBucket() {
        assertThat(statusCheck.check(runtime, jobName)).isEqualTo(StatusCheck.Status.UNKNOWN);
    }
}