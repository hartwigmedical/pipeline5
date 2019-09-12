package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.execution.vm.BucketCompletionWatcher.State;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class BucketCompletionWatcherTest {
    private static final String NAMESPACE = "test/";
    private static final String FAILURE_FLAG = "job.failure";
    private static final String SUCCESS_FLAG = "job.success";

    private MockRuntimeBucket runtimeBucket;
    private List<Blob> blobs;
    private Blob mockBlob;
    private VirtualMachineJobDefinition job;
    private BucketCompletionWatcher victim;

    @Before
    public void setup() {
        runtimeBucket = MockRuntimeBucket.test();
        blobs = new ArrayList<>();
        mockBlob = mock(Blob.class);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(blobs);

        victim = new BucketCompletionWatcher();

        job = mock(VirtualMachineJobDefinition.class);
        final BashStartupScript startupCommand = mock(BashStartupScript.class);
        when(job.startupCommand()).thenReturn(startupCommand);
        when(startupCommand.successFlag()).thenReturn(SUCCESS_FLAG);
        when(startupCommand.failureFlag()).thenReturn(FAILURE_FLAG);
    }

    @Test
    public void shouldReturnFailureStateIfBucketContainsFailureFlag() {
        mockFlagFile(FAILURE_FLAG);
        assertThat(victim.currentState(runtimeBucket.getRuntimeBucket(), job)).isEqualTo(State.FAILURE);
    }

    @Test
    public void shouldReturnSuccessStateIfBucketContainsSuccessFlag() {
        mockFlagFile(SUCCESS_FLAG);
        assertThat(victim.currentState(runtimeBucket.getRuntimeBucket(), job)).isEqualTo(State.SUCCESS);
    }

    @Test
    public void shouldReturnStillWaitingIfBucketContainsNeitherFlag() {
        assertThat(victim.currentState(runtimeBucket.getRuntimeBucket(), job)).isEqualTo(State.STILL_WAITING);
    }

    @Test
    public void shouldWaitAndEventuallyReturnFailureState() {
        mockFlagFile(FAILURE_FLAG);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>(), blobs);
        assertThat(victim.waitForCompletion(runtimeBucket.getRuntimeBucket(), job)).isEqualTo(State.FAILURE);
        verify(runtimeBucket.getRuntimeBucket(), atLeast(2)).list();
    }

    @Test
    public void shouldWaitAndEventuallyReturnSuccessState() {
        mockFlagFile(SUCCESS_FLAG);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>(), blobs);
        assertThat(victim.waitForCompletion(runtimeBucket.getRuntimeBucket(), job)).isEqualTo(State.SUCCESS);
        verify(runtimeBucket.getRuntimeBucket(), atLeast(2)).list();
    }

    private void mockFlagFile(String flagFile) {
        mockReadChannel(mockBlob, NAMESPACE + flagFile);
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(flagFile)).thenReturn(mockBlob);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(blobs);
    }

    private void mockReadChannel(final Blob mockBlob, final String name) {
        ReadChannel mockReadChannel = mock(ReadChannel.class);
        try {
            when(mockReadChannel.read(any())).thenReturn(-1);
        } catch (IOException ioe) {
            fail("Unexpected exception", ioe);
        }
        when(mockBlob.getName()).thenReturn(name);
        when(mockBlob.getSize()).thenReturn(1L);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getMd5()).thenReturn("");
    }
}