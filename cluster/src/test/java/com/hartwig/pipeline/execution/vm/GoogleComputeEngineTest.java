package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.cloud.compute.v1.AttachedDisk;
import com.google.cloud.compute.v1.Error;
import com.google.cloud.compute.v1.Errors;
import com.google.cloud.compute.v1.Image;
import com.google.cloud.compute.v1.ImagesClient;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.Operation;
import com.google.cloud.compute.v1.Scheduling;
import com.google.cloud.compute.v1.Zone;
import com.google.cloud.compute.v1.ZonesClient;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GoogleComputeEngineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String INSTANCE_NAME = "test-test";
    private static final String FIRST_ZONE_NAME = "europe-west4-a";
    private static final String SECOND_ZONE_NAME = "europe-west4-b";
    private GoogleComputeEngine victim;
    private MockRuntimeBucket runtimeBucket;
    private ZonesClient zonesClient;
    private ImagesClient imagesClient;
    private ImmutableVirtualMachineJobDefinition jobDefinition;
    private InstanceLifecycleManager lifecycleManager;
    private BucketCompletionWatcher bucketWatcher;
    private ArgumentCaptor<Instance> instanceArgumentCaptor;

    @Before
    public void setUp() throws Exception {

        zonesClient = mock(ZonesClient.class);
        imagesClient = mock(ImagesClient.class);

        when(imagesClient.getFromFamily(ARGUMENTS.project(), VirtualMachineJobDefinition.STANDARD_IMAGE)).thenReturn(Image.newBuilder()
                .build());

        instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        Operation insertOperation = Operation.newBuilder().setName("insert").build();
        lifecycleManager = mock(InstanceLifecycleManager.class);
        when(lifecycleManager.deleteOldInstancesAndStart(instanceArgumentCaptor.capture(), any(), any())).thenReturn(insertOperation);

        when(lifecycleManager.fetchZones()).thenReturn(List.of(zone(GoogleComputeEngineTest.FIRST_ZONE_NAME),
                zone(GoogleComputeEngineTest.SECOND_ZONE_NAME)));

        bucketWatcher = mock(BucketCompletionWatcher.class);
        victim = new GoogleComputeEngine(ARGUMENTS, imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, Labels.of(Arguments.testDefaults(), TestInputs.defaultSomaticRunMetadata()));
        runtimeBucket = MockRuntimeBucket.test();
        jobDefinition = VirtualMachineJobDefinition.builder()
                .name("test")
                .namespacedResults(RESULTS_DIRECTORY)
                .startupCommand(BashStartupScript.of(runtimeBucket.getRuntimeBucket().name()))
                .build();
    }

    @NotNull
    private Zone zone(final String name) {
        return Zone.newBuilder().setName(name).setRegion(ARGUMENTS.region()).build();
    }

    @Test
    public void createsVmWithRunScriptAndWaitsForCompletion() {
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
        victim = new GoogleComputeEngine(arguments, imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).disableStartupScript(INSTANCE_NAME, FIRST_ZONE_NAME);
    }

    @Test
    public void shouldSkipJobWhenSuccessFlagFileAlreadyExists() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.SUCCESS);
        assertThat(victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition)).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void shouldDeleteStateWhenFailureFlagExists() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.FAILURE);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete(RuntimeFiles.typical().failure());
        verify(runtimeBucket.getRuntimeBucket(), times(1)).delete("results");
    }

    @Test
    public void deletesVmAfterJobIsSuccessful() {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(INSTANCE_NAME, FIRST_ZONE_NAME);
    }

    @Test
    public void attemptsToDeleteLeftoverInstanceOnStartup() {
        returnSuccess();
        Instance mockedInstance = mock(Instance.class);
        when(lifecycleManager.findExistingInstance(INSTANCE_NAME)).thenReturn(Optional.of(mockedInstance));
        when(mockedInstance.getName()).thenReturn(INSTANCE_NAME);
        when(mockedInstance.getZone()).thenReturn(FIRST_ZONE_NAME);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(INSTANCE_NAME, FIRST_ZONE_NAME);
    }

    @Test
    public void attemptsToDeleteRunningSpotInstanceOnStartup() {
        returnSuccess();
        Instance mockedInstance = mock(Instance.class);
        when(lifecycleManager.findExistingInstance(INSTANCE_NAME)).thenReturn(Optional.of(mockedInstance));
        when(mockedInstance.getName()).thenReturn(INSTANCE_NAME);
        when(mockedInstance.getZone()).thenReturn(FIRST_ZONE_NAME);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void attemptsToStopRunningNonPreemptibleInstanceOnStartup() {
        Arguments arguments = Arguments.testDefaultsBuilder().useLocalSsds(false).build();
        victim = new GoogleComputeEngine(arguments, zonesClient, imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        returnSuccess();
        Instance mockedInstance = mock(Instance.class);
        when(lifecycleManager.findExistingInstance(INSTANCE_NAME)).thenReturn(Optional.of(mockedInstance));
        when(mockedInstance.getName()).thenReturn(INSTANCE_NAME);
        when(mockedInstance.getZone()).thenReturn(FIRST_ZONE_NAME);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).stop(FIRST_ZONE_NAME, INSTANCE_NAME);
    }

    @Test
    public void stopsInstanceWithPersistentDisksUponFailure() {
        Arguments arguments = Arguments.testDefaultsBuilder().useLocalSsds(false).build();
        victim = new GoogleComputeEngine(arguments, imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).stop(INSTANCE_NAME, FIRST_ZONE_NAME);
    }

    @Test
    public void deletesInstanceWithLocalSSdsUponFailure() {
        returnFailed();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).delete(INSTANCE_NAME, FIRST_ZONE_NAME);
    }

    @Test
    public void usesPublicNetworkIfNoPrivateSpecified() {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        final Instance value = instanceArgumentCaptor.getValue();
        List<NetworkInterface> networkInterfaces = value.getNetworkInterfacesList();
        assertThat(networkInterfaces.size()).isEqualTo(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo("projects/hmf-pipeline-development/global/networks/default");
    }

    @Test
    public void usesNetworkAndSubnetWhenSpecified() {
        returnSuccess();
        victim = new GoogleComputeEngine(Arguments.testDefaultsBuilder().network("private").subnet("subnet").build(),
                imagesClient,
                z -> {
                },
                lifecycleManager,
                bucketWatcher,
                mock(Labels.class));
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);

        List<NetworkInterface> networkInterfaces = instanceArgumentCaptor.getValue().getNetworkInterfacesList();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo("projects/hmf-pipeline-development/global/networks/private");
        assertThat(networkInterfaces.get(0).getSubnetwork()).isEqualTo(
                "projects/hmf-pipeline-development/regions/europe-west4/subnetworks/subnet");
    }

    @Test
    public void usesNetworkAsSubnetWhenNotSpecified() {
        returnSuccess();
        victim = new GoogleComputeEngine(Arguments.testDefaultsBuilder().network("private").build(), imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        List<NetworkInterface> networkInterfaces = instanceArgumentCaptor.getValue().getNetworkInterfacesList();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo("projects/hmf-pipeline-development/global/networks/private");
        assertThat(networkInterfaces.get(0).getSubnetwork()).isEqualTo(
                "projects/hmf-pipeline-development/regions/europe-west4/subnetworks/private");
    }

    @Test
    public void usesFullNetworkAndSubnetWhenSpecified() {
        returnSuccess();
        String networkUrl = "projects/private";
        String subnetUrl = "projects/subnet";
        victim = new GoogleComputeEngine(Arguments.testDefaultsBuilder().network(networkUrl).subnet(subnetUrl).build(),
                imagesClient,
                z -> {
                },
                lifecycleManager,
                bucketWatcher,
                mock(Labels.class));
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);

        List<NetworkInterface> networkInterfaces = instanceArgumentCaptor.getValue().getNetworkInterfacesList();
        assertThat(networkInterfaces).hasSize(1);
        assertThat(networkInterfaces.get(0).getNetwork()).isEqualTo(networkUrl);
        assertThat(networkInterfaces.get(0).getSubnetwork()).isEqualTo(subnetUrl);
    }

    @Test
    public void addsTagsToComputeEngineInstances() {
        returnSuccess();
        victim = new GoogleComputeEngine(Arguments.testDefaultsBuilder().tags(List.of("tag")).build(), imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        assertThat(instanceArgumentCaptor.getValue().getTags().getItemsList()).containsExactly("tag");
    }

    @Test
    public void triesMultipleZonesWhenResourcesExhausted() {
        Operation resourcesExhausted = operationWithError(GoogleComputeEngine.ZONE_EXHAUSTED_ERROR_CODE);
        when(lifecycleManager.deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(FIRST_ZONE_NAME))).thenReturn(resourcesExhausted,
                Operation.newBuilder().build());
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(SECOND_ZONE_NAME));
    }

    @NotNull
    private Operation operationWithError(final String code) {
        return Operation.newBuilder()
                .setStatus(Operation.Status.DONE)
                .setName("insert")
                .setError(Error.newBuilder().addErrors(Errors.newBuilder().setCode(code).build()))
                .build();
    }

    @Test
    public void triesMultipleZonesWhenUnsupportedOperation() {
        Operation resourcesExhausted = operationWithError(GoogleComputeEngine.UNSUPPORTED_OPERATION_ERROR_CODE);
        when(lifecycleManager.deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(FIRST_ZONE_NAME))).thenReturn(resourcesExhausted,
                Operation.newBuilder().build());
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(SECOND_ZONE_NAME));
    }

    @Test
    public void setsVmsToSpotWhenFlagEnabled() {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        Scheduling scheduling = instanceArgumentCaptor.getValue().getScheduling();
        assertThat(scheduling.getProvisioningModel()).isEqualTo("SPOT");
    }

    @Test
    public void restartsPreemptedInstanceInNextZone() {
        when(lifecycleManager.instanceStatus(any(), any())).thenReturn(GoogleComputeEngine.PREEMPTED_INSTANCE);
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.SUCCESS);
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(lifecycleManager).deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(FIRST_ZONE_NAME));
        verify(lifecycleManager).deleteOldInstancesAndStart(any(), eq(INSTANCE_NAME), eq(SECOND_ZONE_NAME));
    }

    @Test
    public void attachesLocalSsdsWhenEnabled() {
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        List<AttachedDisk> disks = instanceArgumentCaptor.getValue().getDisksList();
        assertThat(disks).hasSize(5);
        assertThat(disks.get(0).getInitializeParams().getDiskType()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/zones/europe-west4-a/diskTypes/pd-ssd");
        assertThat(disks.get(0).getInitializeParams().getDiskSizeGb()).isEqualTo(200L);
        isLocalSSD(disks.get(1));
        isLocalSSD(disks.get(2));
        isLocalSSD(disks.get(3));
        isLocalSSD(disks.get(4));
    }

    @Test
    public void attachesTwoPersisentDisksWhenLocalSSDDisabled() {
        victim = new GoogleComputeEngine(Arguments.builder().from(ARGUMENTS).useLocalSsds(false).build(), imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        List<AttachedDisk> disks = instanceArgumentCaptor.getValue().getDisksList();
        assertThat(disks).hasSize(2);
        assertThat(disks.get(0).getInitializeParams().getDiskType()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/zones/europe-west4-a/diskTypes/pd-ssd");
        assertThat(disks.get(0).getInitializeParams().getDiskSizeGb()).isEqualTo(200L);
        assertThat(disks.get(1).getInitializeParams().getDiskType()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/zones/europe-west4-a/diskTypes/pd-ssd");
        assertThat(disks.get(1).getInitializeParams().getDiskSizeGb()).isEqualTo(1200L);
    }

    @Test
    public void usesLatestImageFromCurrentFamilyWhenNoImageGiven() {
        victim = new GoogleComputeEngine(ARGUMENTS, imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(imagesClient).getFromFamily(ARGUMENTS.project(), VirtualMachineJobDefinition.STANDARD_IMAGE);
    }

    @Test
    public void usesLatestImageFromCurrentFamilyWithGivenProject() {
        String givenProject = "givenProject";
        victim = new GoogleComputeEngine(Arguments.builder().from(ARGUMENTS).imageProject(givenProject).build(),
                imagesClient,
                z -> {
                },
                lifecycleManager,
                bucketWatcher,
                mock(Labels.class));
        returnSuccess();
        ArgumentCaptor<String> project = ArgumentCaptor.forClass(String.class);
        when(imagesClient.getFromFamily(project.capture(), eq(VirtualMachineJobDefinition.STANDARD_IMAGE))).thenReturn(Image.newBuilder()
                .build());
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        assertThat(project.getValue()).isEqualTo(givenProject);
    }

    @Test
    public void usesGivenImageFromPublicImageProjectWhenProvided() {
        String imageName = "alternate_image";
        Image specificImage = Image.newBuilder().setName(imageName).setSelfLink(imageName).build();
        victim = new GoogleComputeEngine(Arguments.testDefaultsBuilder().imageName(imageName).build(), imagesClient, z -> {
        }, lifecycleManager, bucketWatcher, mock(Labels.class));
        when(imagesClient.get(VirtualMachineJobDefinition.HMF_IMAGE_PROJECT, imageName)).thenReturn(specificImage);
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        verify(imagesClient, never()).getFromFamily(any(), any());
        assertThat(instanceArgumentCaptor.getValue().getDisksList().get(0).getInitializeParams().getSourceImage()).isEqualTo(imageName);
    }

    @Test
    public void appliesLabelsToInstanceAndDisks() {
        victim = new GoogleComputeEngine(ARGUMENTS,
                imagesClient,
                z -> {
                },
                lifecycleManager,
                bucketWatcher,
                Labels.of(Arguments.testDefaultsBuilder().userLabel("username").costCenterLabel("development").build(),
                        TestInputs.defaultSomaticRunMetadata()));
        returnSuccess();
        victim.submit(runtimeBucket.getRuntimeBucket(), jobDefinition);
        List<AttachedDisk> disks = instanceArgumentCaptor.getValue().getDisksList();
        final Map<String, String> appliedLabels =
                Map.of("cost_center", "development", "job_name", "test", "run_id", "test", "sample", "tumor", "user", "username");
        assertThat(disks.get(0).getInitializeParams().getLabelsMap()).isEqualTo(appliedLabels);
    }

    public void isLocalSSD(final AttachedDisk disk) {
        assertThat(disk.getInitializeParams().getDiskType()).isEqualTo(
                "https://www.googleapis.com/compute/v1/projects/hmf-pipeline-development/zones/europe-west4-a/diskTypes/local-ssd");
    }

    private void returnSuccess() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.SUCCESS);
        Operation successOperation = Operation.newBuilder().build();
        when(lifecycleManager.deleteOldInstancesAndStart(instanceArgumentCaptor.capture(), any(), any())).thenReturn(successOperation);
    }

    private void returnFailed() {
        when(bucketWatcher.currentState(any(), any())).thenReturn(BucketCompletionWatcher.State.STILL_WAITING,
                BucketCompletionWatcher.State.FAILURE);
    }
}