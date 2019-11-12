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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BucketCompletionWatcher.State;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class ComputeEngineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String INSTANCE_NAME = "test-test";
    private static final String DONE = "DONE";
    private static final String FIRST_ZONE_NAME = "europe-west4-a";
    private static final String SECOND_ZONE_NAME = "europe-west4-b";
    private ComputeEngine victim;
    private MockRuntimeBucket runtimeBucket;
    private Compute compute;
    private ImmutableVirtualMachineJobDefinition jobDefinition;
    private Instance instance;
    private Compute.Instances instances;
    private Compute.ZoneOperations zoneOperations;
    private Compute.ZoneOperations.Get zoneOpGet;
    private ArgumentCaptor<Instance> instanceArgumentCaptor;
    private InstanceLifecycleManager lifecycleManager;
    private BucketCompletionWatcher bucketWatcher;

    @Before
    public void setUp() throws Exception {
        Compute.Images images = mock(Compute.Images.class);
        Compute.Images.GetFromFamily getFromFamily = mock(Compute.Images.GetFromFamily.class);
        when(getFromFamily.execute()).thenReturn(new Image());
        when(images.getFromFamily(ARGUMENTS.project(), VirtualMachineJobDefinition.STANDARD_IMAGE)).thenReturn(getFromFamily);

        instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        Operation insertOperation = mock(Operation.class);
        when(insertOperation.getName()).thenReturn("insert");
        instances = mock(Compute.Instances.class);
        lifecycleManager = mock(InstanceLifecycleManager.class);
        instance = mock(Instance.class);
        when(lifecycleManager.newInstance()).thenReturn(instance);
        when(lifecycleManager.deleteOldInstancesAndStart(instanceArgumentCaptor.capture(), any(), any())).thenReturn(insertOperation);
        when(instance.getName()).thenReturn(INSTANCE_NAME);
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

        Compute.Instances.List list = mock(Compute.Instances.List.class);
        InstanceList instanceList = mock(InstanceList.class);
        Instance one = mock(Instance.class);
        Instance two = mock(Instance.class);
        Instance three = mock(Instance.class);
        when(one.getName()).thenReturn("vm-1");
        when(two.getName()).thenReturn("vm-2");
        when(three.getName()).thenReturn("vm-3");
        List<Instance> existingInstances = Arrays.asList(one, two, three);
        when(instances.list(any(), any())).thenReturn(list);
        when(list.execute()).thenReturn(instanceList);
        when(instanceList.getItems()).thenReturn(existingInstances);

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

        bucketWatcher = mock(BucketCompletionWatcher.class);
        victim = new ComputeEngine(ARGUMENTS, compute, z -> {}, lifecycleManager, bucketWatcher);
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
    public void returnsJobFailedWhenScriptFailsRemotely() {
        returnFailed();
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void disablesStartupScriptWhenInstanceWithPersistentDisksFailsRemotely() throws Exception {
        Arguments arguments = Arguments.testDefaultsBuilder().useLocalSsds(false).build();
        victim = new ComputeEngine(arguments, compute, z -> {
        }, lifecycleManager, bucketWatcher);
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).disableStartupScript(FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void shouldSkipJobWhenSuccessFlagFileAlreadyExists() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.SUCCESS);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.SKIPPED);
        verifyNoMoreInteractions(compute);
    }

    @Test
    public void shouldDeleteStateWhenFailureFlagExists() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.FAILURE);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete(BashStartupScript.JOB_FAILED_FLAG);
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete("results");
    }

    @Test
    public void deletesVmAfterJobIsSuccessful() throws Exception {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(FIRST_ZONE_NAME, INSTANCE_NAME);
    }


    @Test
    public void stopsInstanceWithPersistentDisksUponFailure() {
        Arguments arguments = Arguments.testDefaultsBuilder().useLocalSsds(false).build();
        victim = new ComputeEngine(arguments, compute, z -> {
        }, lifecycleManager, bucketWatcher);
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).stop(FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void deletesInstanceWithLocalSSdsUponFailure() {
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void usesPublicNetworkIfNoPrivateSpecified() throws Exception {
        returnSuccess();
        ArgumentCaptor<List<NetworkInterface>> captor = ArgumentCaptor.forClass(List.class);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);

        verify(instance).setNetworkInterfaces(captor.capture());
        List<NetworkInterface> networkInterfaces = captor.getValue();
        assertThat(networkInterfaces.size()).isEqualTo(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/global/networks/default");
    }

    @Test
    public void usesPrivateNetworkWhenSpecified() throws Exception {
        returnSuccess();
        victim = new ComputeEngine(Arguments.testDefaultsBuilder().privateNetwork("private").build(), compute, z -> {
        }, lifecycleManager, bucketWatcher);
        ArgumentCaptor<List<NetworkInterface>> interfaceCaptor = ArgumentCaptor.forClass(List.class);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);

        verify(instance).setNetworkInterfaces(interfaceCaptor.capture());
        List<NetworkInterface> networkInterfaces = interfaceCaptor.getValue();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/global/networks/private");
        assertThat(networkInterfaces.get(0).getSubnetwork()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/regions/europe-west4/subnetworks/private");
        assertThat(networkInterfaces.get(0).get("no-address")).isEqualTo("true");
    }

    @Test
    public void triesMultipleZonesWhenResourcesExhausted() throws Exception {
        Operation resourcesExhausted = new Operation().setStatus("DONE").setName("insert")
                .setError(new Operation.Error().setErrors(Collections.singletonList(
                        new Operation.Error.Errors().setCode(ComputeEngine.ZONE_EXHAUSTED_ERROR_CODE))));
        when(lifecycleManager.deleteOldInstancesAndStart(instance, FIRST_ZONE_NAME, INSTANCE_NAME))
                .thenReturn(resourcesExhausted, mock(Operation.class));
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.STILL_WAITING, State.STILL_WAITING, State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(instance, SECOND_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void triesMultipleZonesWhenUnsupportedOperation() throws Exception {
        Operation resourcesExhausted = new Operation().setStatus("DONE").setName("insert")
                .setError(new Operation.Error().setErrors(Collections.singletonList(
                        new Operation.Error.Errors().setCode(ComputeEngine.UNSUPPORTED_OPERATION_ERROR_CODE))));
        when(lifecycleManager.deleteOldInstancesAndStart(instance, FIRST_ZONE_NAME, INSTANCE_NAME))
                .thenReturn(resourcesExhausted, mock(Operation.class));
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.STILL_WAITING, State.STILL_WAITING, State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(instance, SECOND_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void setsVmsToPreemptibleWhenFlagEnabled() throws Exception {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(instance).setScheduling(eq(new Scheduling().setPreemptible(true)));
    }

    @Test
    public void restartsPreemptedInstanceInNextZone() throws Exception {
        when(lifecycleManager.instanceStatus(any(), any())).thenReturn(ComputeEngine.PREEMPTED_INSTANCE);
        when(bucketWatcher.currentState(any(), any()))
                .thenReturn(State.STILL_WAITING, State.STILL_WAITING, State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(instance, FIRST_ZONE_NAME, INSTANCE_NAME);
        verify(lifecycleManager).deleteOldInstancesAndStart(instance, SECOND_ZONE_NAME, INSTANCE_NAME);
    }

    private void returnSuccess() throws IOException {
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.STILL_WAITING, State.SUCCESS);
        Operation successOperation = mock(Operation.class);
        when(lifecycleManager.deleteOldInstancesAndStart(any(), any(), any())).thenReturn(successOperation);
    }

    private void returnFailed() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(State.STILL_WAITING, State.FAILURE);
    }
}