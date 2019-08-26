package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class ComputeEngineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String NAMESPACE = "test/";
    private static final String INSTANCE_NAME = "test-test";
    private static final String DONE = "DONE";
    private static final String FIRST_ZONE_NAME = "europe-west4-a";
    private static final String SECOND_ZONE_NAME = "europe-west4-b";
    private ComputeEngine victim;
    private MockRuntimeBucket runtimeBucket;
    private Compute compute;
    private ImmutableVirtualMachineJobDefinition jobDefinition;
    private Compute.Instances instances;
    private Compute.ZoneOperations zoneOperations;
    private Compute.Instances.Insert insert;
    private Compute.ZoneOperations.Get zoneOpGet;

    @Before
    public void setUp() throws Exception {
        Compute.Images images = mock(Compute.Images.class);
        Compute.Images.GetFromFamily getFromFamily = mock(Compute.Images.GetFromFamily.class);
        when(getFromFamily.execute()).thenReturn(new Image());
        when(images.getFromFamily(ARGUMENTS.project(), VirtualMachineJobDefinition.STANDARD_IMAGE)).thenReturn(getFromFamily);

        ArgumentCaptor<Instance> instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        insert = mock(Compute.Instances.Insert.class);
        Operation insertOperation = mock(Operation.class);
        when(insertOperation.getName()).thenReturn("insert");
        instances = mock(Compute.Instances.class);
        when(instances.insert(eq(ARGUMENTS.project()), eq(FIRST_ZONE_NAME), instanceArgumentCaptor.capture())).thenReturn(insert);
        when(insert.execute()).thenReturn(insertOperation);
        Compute.Instances.Stop stop = mock(Compute.Instances.Stop.class);
        Operation stopOperation = mock(Operation.class);
        when(stopOperation.getName()).thenReturn("stop");
        when(stopOperation.getStatus()).thenReturn(DONE);
        when(stop.execute()).thenReturn(stopOperation);
        when(instances.stop(ARGUMENTS.project(), FIRST_ZONE_NAME, INSTANCE_NAME)).thenReturn(stop);

        Compute.Instances.Delete delete = mock(Compute.Instances.Delete.class);
        Operation deleteOperation = mock(Operation.class);
        when(deleteOperation.getName()).thenReturn("delete");
        when(deleteOperation.getStatus()).thenReturn(DONE);
        when(delete.execute()).thenReturn(stopOperation);
        when(instances.delete(ARGUMENTS.project(), FIRST_ZONE_NAME, INSTANCE_NAME)).thenReturn(delete);

        zoneOperations = mock(Compute.ZoneOperations.class);
        zoneOpGet = mock(Compute.ZoneOperations.Get.class);
        Operation zoneOpGetOperation = mock(Operation.class);
        when(zoneOpGetOperation.getStatus()).thenReturn(DONE);
        when(zoneOpGet.execute()).thenReturn(zoneOpGetOperation);
        when(zoneOperations.get(ARGUMENTS.project(), FIRST_ZONE_NAME, "insert")).thenReturn(zoneOpGet);
        when(zoneOperations.get(ARGUMENTS.project(), FIRST_ZONE_NAME, "stop")).thenReturn(zoneOpGet);

        compute = mock(Compute.class);
        when(compute.images()).thenReturn(images);
        when(compute.instances()).thenReturn(instances);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        Compute.Zones zones = mock(Compute.Zones.class);
        Compute.Zones.List zonesList = mock(Compute.Zones.List.class);
        when(zonesList.execute()).thenReturn(new ZoneList().setItems(Lists.newArrayList(zone(FIRST_ZONE_NAME), zone(SECOND_ZONE_NAME))));
        when(zones.list(ARGUMENTS.project())).thenReturn(zonesList);
        when(compute.zones()).thenReturn(zones);

        victim = new ComputeEngine(ARGUMENTS, compute);
        runtimeBucket = MockRuntimeBucket.test();
        jobDefinition = VirtualMachineJobDefinition.builder()
                .name("test")
                .namespacedResults(RESULTS_DIRECTORY)
                .startupCommand(BashStartupScript.of(runtimeBucket.getRuntimeBucket().name()))
                .build();
    }

    private Zone zone(final String name) {
        return new Zone().setName(name).setRegion(ARGUMENTS.region());
    }

    @Test
    public void returnsStatusFailedOnUncaughtException() {
        when(compute.instances()).thenThrow(new NullPointerException());
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void createsVmWithRunScriptAndWaitsForCompletion() throws Exception {
        returnSuccess();
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void returnsJobFailedWhenScriptFailsRemotely() throws Exception {
        returnFailed();
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void disablesStartupScriptWhenScriptFailsRemotely() throws Exception {
        returnFailed();
        ArgumentCaptor<Metadata> newMetadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        Compute.Instances.SetMetadata setMetadata = mock(Compute.Instances.SetMetadata.class);
        Operation setMetadataOperation = mock(Operation.class);
        String opName = "setMetadata";
        when(setMetadataOperation.getName()).thenReturn(opName);
        when(setMetadataOperation.getStatus()).thenReturn(DONE);
        when(setMetadata.execute()).thenReturn(setMetadataOperation);
        when(instances.setMetadata(eq(ARGUMENTS.project()),
                eq(FIRST_ZONE_NAME),
                eq(INSTANCE_NAME),
                newMetadataCaptor.capture())).thenReturn(setMetadata);
        when(zoneOperations.get(ARGUMENTS.project(), FIRST_ZONE_NAME, opName)).thenReturn(zoneOpGet);

        Compute.Instances.Get get = mock(Compute.Instances.Get.class);
        String fingerprint = "fingerprint";
        Instance latest = new Instance().setMetadata(new Metadata().setFingerprint(fingerprint));
        when(get.execute()).thenReturn(latest);
        when(instances.get(ARGUMENTS.project(), FIRST_ZONE_NAME, INSTANCE_NAME)).thenReturn(get);

        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        assertThat(newMetadataCaptor.getValue().getItems()).isNull();
        assertThat(newMetadataCaptor.getValue().getFingerprint()).isEqualTo(fingerprint);
    }

    @Test
    public void shouldSkipJobWhenSuccessFlagFileAlreadyExists() {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.SKIPPED);
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
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instances, times(1)).delete(ARGUMENTS.project(), FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void retriesStopDeleteOperationsOnFailures() throws Exception {
        returnSuccess();
        Compute.Instances.Delete goingToFailOnce = mock(Compute.Instances.Delete.class);
        Operation operation = mock(Operation.class);
        when(goingToFailOnce.execute()).thenThrow(new IOException()).thenReturn(operation);
        when(instances.delete(ARGUMENTS.project(), FIRST_ZONE_NAME, INSTANCE_NAME)).thenReturn(goingToFailOnce);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(goingToFailOnce, times(2)).execute();
    }

    @Test
    public void usesPublicNetworkIfNoPrivateSpecified() throws Exception {
        returnSuccess();
        ArgumentCaptor<Instance> instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instances).insert(any(), any(), instanceArgumentCaptor.capture());
        List<NetworkInterface> networkInterfaces = instanceArgumentCaptor.getValue().getNetworkInterfaces();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/global/networks/default");
    }

    @Test
    public void usesPrivateNetworkWhenSpecified() throws Exception {
        returnSuccess();
        victim = new ComputeEngine(Arguments.testDefaultsBuilder().privateNetwork("private").build(), compute);
        ArgumentCaptor<Instance> instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instances).insert(any(), any(), instanceArgumentCaptor.capture());
        List<NetworkInterface> networkInterfaces = instanceArgumentCaptor.getValue().getNetworkInterfaces();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/global/networks/private");
        assertThat(networkInterfaces.get(0).getSubnetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/regions/europe-west4/subnetworks/private");
        assertThat(networkInterfaces.get(0).get("no-address")).isEqualTo("true");
    }

    @Test
    public void triesMultipleZonesWhenResourcesExhausted() throws Exception {
        Operation resourcesExhaused = new Operation().setStatus("DONE")
                .setName("insert")
                .setError(new Operation.Error().setErrors(Collections.singletonList(new Operation.Error.Errors().setCode(ComputeEngine.ZONE_EXHAUSTED_ERROR_CODE))));
        when(insert.execute()).thenReturn(resourcesExhaused);
        when(zoneOpGet.execute()).thenReturn(resourcesExhaused);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instances, times(1)).insert(eq(ARGUMENTS.project()), eq(SECOND_ZONE_NAME), any());
    }

    private void returnSuccess() throws IOException {
        runtimeBucket = runtimeBucket.with(successBlob(), 1);
        List<Blob> blobs = new ArrayList<>();
        Blob mockBlob = mock(Blob.class);
        mockReadChannel(mockBlob, successBlob());
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(BashStartupScript.JOB_SUCCEEDED_FLAG)).thenReturn(mockBlob);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>()).thenReturn(blobs);
    }

    private void returnFailed() throws IOException {
        runtimeBucket = runtimeBucket.with(failureBlob(), 1);
        List<Blob> blobs = new ArrayList<>();
        Blob mockBlob = mock(Blob.class);
        mockReadChannel(mockBlob, failureBlob());
        blobs.add(mockBlob);
        when(runtimeBucket.getRuntimeBucket().get(BashStartupScript.JOB_FAILED_FLAG)).thenReturn(mockBlob);
        when(runtimeBucket.getRuntimeBucket().list()).thenReturn(new ArrayList<>()).thenReturn(new ArrayList<>()).thenReturn(blobs);
    }

    private void mockReadChannel(final Blob mockBlob, final String value2) throws IOException {
        ReadChannel mockReadChannel = mock(ReadChannel.class);
        when(mockReadChannel.read(any())).thenReturn(-1);
        when(mockBlob.getName()).thenReturn(value2);
        when(mockBlob.getSize()).thenReturn(1L);
        when(mockBlob.reader()).thenReturn(mockReadChannel);
        when(mockBlob.getMd5()).thenReturn("");
    }

    private String successBlob() {
        return NAMESPACE + BashStartupScript.JOB_SUCCEEDED_FLAG;
    }

    private String failureBlob() {
        return NAMESPACE + BashStartupScript.JOB_FAILED_FLAG;
    }
}