package com.hartwig.pipeline.execution.vm;

import com.google.api.gax.paging.Page;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Operation;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.NamespacedResults;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class ComputeEngineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final NamespacedResults NAMESPACED_RESULTS = NamespacedResults.of("test");
    private ComputeEngine victim;
    private MockRuntimeBucket runtimeBucket;
    private Compute compute;
    private ImmutableVirtualMachineJobDefinition jobDefinition;

    @Before
    public void setUp() throws Exception {
        Compute.Images images = mock(Compute.Images.class);
        Compute.Images.GetFromFamily getFromFamily = mock(Compute.Images.GetFromFamily.class);
        when(getFromFamily.execute()).thenReturn(new Image());
        when(images.getFromFamily(ARGUMENTS.project(), VirtualMachineJobDefinition.STANDARD_IMAGE)).thenReturn(getFromFamily);

        ArgumentCaptor<Instance> instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        Compute.Instances.Insert insert = mock(Compute.Instances.Insert.class);
        Operation insertOperation = mock(Operation.class);
        when(insertOperation.getName()).thenReturn("insert");
        Compute.Instances instances = mock(Compute.Instances.class);
        when(instances.insert(eq(ARGUMENTS.project()), eq(ComputeEngine.ZONE_NAME), instanceArgumentCaptor.capture())).thenReturn(insert);
        when(insert.execute()).thenReturn(insertOperation);
        Compute.Instances.Stop stop = mock(Compute.Instances.Stop.class);
        Operation stopOperation = mock(Operation.class);
        when(stopOperation.getName()).thenReturn("stop");
        when(stopOperation.getStatus()).thenReturn("DONE");
        when(stop.execute()).thenReturn(stopOperation);
        when(instances.stop(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "test-test")).thenReturn(stop);

        Compute.ZoneOperations zoneOperations = mock(Compute.ZoneOperations.class);
        Compute.ZoneOperations.Get zoneOpGet = mock(Compute.ZoneOperations.Get.class);
        Operation zoneOpGetOperation = mock(Operation.class);
        when(zoneOpGetOperation.getStatus()).thenReturn("DONE");
        when(zoneOpGet.execute()).thenReturn(zoneOpGetOperation);
        when(zoneOperations.get(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "insert")).thenReturn(zoneOpGet);
        when(zoneOperations.get(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "stop")).thenReturn(zoneOpGet);

        compute = mock(Compute.class);
        when(compute.images()).thenReturn(images);
        when(compute.instances()).thenReturn(instances);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        victim = new ComputeEngine(ARGUMENTS, compute);
        runtimeBucket = MockRuntimeBucket.test();
        jobDefinition = VirtualMachineJobDefinition.builder()
                .name("test")
                .namespacedResults(NAMESPACED_RESULTS)
                .startupCommand(BashStartupScript.of(runtimeBucket.getRuntimeBucket().name()))
                .build();
    }

    @Test
    public void returnsStatusFailedOnUncaughtException() {
        when(compute.instances()).thenThrow(new NullPointerException());
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void createsVmWithRunScriptAndWaitsForCompletion() {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        Bucket underlyingBucket = runtimeBucket.getRuntimeBucket().bucket();

        Page page = mock(Page.class);
        //noinspection unchecked
        when(page.iterateAll()).thenReturn(new ArrayList<>());

        Page withFlag = mock(Page.class);
        List<Blob> blobs = new ArrayList<>();
        try {
            Blob mockBlob = mock(Blob.class);
            ReadChannel mockReadChannel = mock(ReadChannel.class);
            when(mockReadChannel.read(any())).thenReturn(-1);
            when(mockBlob.getName()).thenReturn(successBlob());
            when(mockBlob.getSize()).thenReturn(1L);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getMd5()).thenReturn("");
            blobs.add(mockBlob);
            when(underlyingBucket.get(BashStartupScript.JOB_SUCCEEDED_FLAG)).thenReturn(mockBlob);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        when(withFlag.iterateAll()).thenReturn(blobs);
        when(underlyingBucket.list(any())).thenReturn(page).thenReturn(page).thenReturn(withFlag);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.SUCCESS);
    }

    private String successBlob() {
        return NAMESPACED_RESULTS.path(BashStartupScript.JOB_SUCCEEDED_FLAG);
    }

    @Test
    public void returnsJobFailedWhenScriptFailsRemotely() {
        runtimeBucket = runtimeBucket.with(failureBlob(), 1);
        Bucket underlyingBucket = runtimeBucket.getRuntimeBucket().bucket();

        Page page = mock(Page.class);
        //noinspection unchecked
        when(page.iterateAll()).thenReturn(new ArrayList<>());

        Page withFlag = mock(Page.class);
        List<Blob> blobs = new ArrayList<>();
        try {
            Blob mockBlob = mock(Blob.class);
            ReadChannel mockReadChannel = mock(ReadChannel.class);
            when(mockReadChannel.read(any())).thenReturn(-1);
            when(mockBlob.getName()).thenReturn(failureBlob());
            when(mockBlob.getSize()).thenReturn(1L);
            when(mockBlob.reader()).thenReturn(mockReadChannel);
            when(mockBlob.getMd5()).thenReturn("");
            blobs.add(mockBlob);
            when(underlyingBucket.get(BashStartupScript.JOB_FAILED_FLAG)).thenReturn(mockBlob);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        when(withFlag.iterateAll()).thenReturn(blobs);
        when(underlyingBucket.list(any())).thenReturn(page).thenReturn(page).thenReturn(withFlag);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.FAILED);
    }



    @Test
    public void shouldSkipJobWhenSuccessFlagFileAlreadyExists() {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.SKIPPED);
        verifyNoMoreInteractions(compute);
    }

    @Test
    public void shouldSkipJobWhenFailureFlagFileAlreadyExists() {
        runtimeBucket = runtimeBucket.with(failureBlob(), 1);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.SKIPPED);
        verifyNoMoreInteractions(compute);
    }

    private String failureBlob() {
        return NAMESPACED_RESULTS.path(BashStartupScript.JOB_FAILED_FLAG);
    }
}