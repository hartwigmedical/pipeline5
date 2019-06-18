package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Operation;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class ComputeEngineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String NAMESPACE = "test/";
    private ComputeEngine victim;
    private MockRuntimeBucket runtimeBucket;
    private Compute compute;
    private ImmutableVirtualMachineJobDefinition jobDefinition;
    private Compute.Instances instances;

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
        instances = mock(Compute.Instances.class);
        when(instances.insert(eq(ARGUMENTS.project()), eq(ComputeEngine.ZONE_NAME), instanceArgumentCaptor.capture())).thenReturn(insert);
        when(insert.execute()).thenReturn(insertOperation);
        Compute.Instances.Stop stop = mock(Compute.Instances.Stop.class);
        Operation stopOperation = mock(Operation.class);
        when(stopOperation.getName()).thenReturn("stop");
        when(stopOperation.getStatus()).thenReturn("DONE");
        when(stop.execute()).thenReturn(stopOperation);
        when(instances.stop(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "test-test")).thenReturn(stop);

        Compute.Instances.Delete delete = mock(Compute.Instances.Delete.class);
        Operation deleteOperation = mock(Operation.class);
        when(deleteOperation.getName()).thenReturn("delete");
        when(deleteOperation.getStatus()).thenReturn("DONE");
        when(delete.execute()).thenReturn(stopOperation);
        when(instances.delete(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "test-test")).thenReturn(delete);

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
                .namespacedResults(RESULTS_DIRECTORY)
                .startupCommand(BashStartupScript.of(runtimeBucket.getRuntimeBucket().name()))
                .build();
    }

    @Test
    public void returnsStatusFailedOnUncaughtException() {
        when(compute.instances()).thenThrow(new NullPointerException());
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void createsVmWithRunScriptAndWaitsForCompletion() throws Exception {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        List<Blob> blobs = new ArrayList<>();
        Blob mockBlob = mock(Blob.class);
        ReadChannel mockReadChannel = mock(ReadChannel.class);
        when(mockReadChannel.read(any())).thenReturn(-1);
        when(mockBlob.getName()).thenReturn(successBlob());
        when(mockBlob.getSize()).thenReturn(1L);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getMd5()).thenReturn("");
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(BashStartupScript.JOB_SUCCEEDED_FLAG)).thenReturn(mockBlob);

        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>()).thenReturn(blobs);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.SUCCESS);
    }

    @Test
    public void returnsJobFailedWhenScriptFailsRemotely() throws Exception {
        runtimeBucket = runtimeBucket.with(failureBlob(), 1);

        List<Blob> blobs = new ArrayList<>();
        Blob mockBlob = mock(Blob.class);
        ReadChannel mockReadChannel = mock(ReadChannel.class);
        when(mockReadChannel.read(any())).thenReturn(-1);
        when(mockBlob.getName()).thenReturn(failureBlob());
        when(mockBlob.getSize()).thenReturn(1L);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getMd5()).thenReturn("");
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(BashStartupScript.JOB_FAILED_FLAG)).thenReturn(mockBlob);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>()).thenReturn(blobs);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void shouldSkipJobWhenSuccessFlagFileAlreadyExists() {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(JobStatus.SKIPPED);
        verifyNoMoreInteractions(compute);
    }

    @Test
    public void shouldDeleteStateWhenFailureFlagExists() {
        runtimeBucket = runtimeBucket.with(failureBlob(), 1);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete(BashStartupScript.JOB_FAILED_FLAG);
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete("results");
    }

    @Test
    public void deletesVmWhenJobIsSuccessful() throws Exception {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        List<Blob> blobs = new ArrayList<>();
        Blob mockBlob = mock(Blob.class);
        ReadChannel mockReadChannel = mock(ReadChannel.class);
        when(mockReadChannel.read(any())).thenReturn(-1);
        when(mockBlob.getName()).thenReturn(successBlob());
        when(mockBlob.getSize()).thenReturn(1L);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getMd5()).thenReturn("");
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(BashStartupScript.JOB_SUCCEEDED_FLAG)).thenReturn(mockBlob);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>()).thenReturn(blobs);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instances, times(1)).delete(ARGUMENTS.project(), ComputeEngine.ZONE_NAME, "test-test");
    }

    private String successBlob() {
        return NAMESPACE + BashStartupScript.JOB_SUCCEEDED_FLAG;
    }

    private String failureBlob() {
        return NAMESPACE + BashStartupScript.JOB_FAILED_FLAG;
    }
}