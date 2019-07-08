package com.hartwig.pipeline.execution.dataproc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.JobReference;
import com.google.api.services.dataproc.v1beta2.model.JobStatus;
import com.google.api.services.dataproc.v1beta2.model.Operation;
import com.google.api.services.dataproc.v1beta2.model.SubmitJobRequest;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GoogleDataprocTest {

    private static final String REGION = "region";
    private static final String PROJECT = "project";
    private static final Arguments ARGUMENTS = Arguments.testDefaultsBuilder().project(PROJECT).region(REGION).build();
    private static final String JOB_ID_AND_CLUSTER_NAME = "sample-gunzip";
    private static final SparkJobDefinition JOB_DEFINITION =
            SparkJobDefinition.gunzip(JarLocation.of("jar"), MockRuntimeBucket.test().getRuntimeBucket());
    private Dataproc.Projects.Regions.Clusters clusters;
    private Dataproc.Projects.Regions.Jobs jobs;
    private GoogleDataproc victim;
    private Dataproc.Projects.Regions.Jobs.Get getJobsRequest;
    private ArgumentCaptor<SubmitJobRequest> submitRequestCaptor;
    private RuntimeBucket runtimeBucket;

    @Before
    public void setUp() throws Exception {
        final Dataproc dataproc = mock(Dataproc.class);
        final Dataproc.Projects projects = mock(Dataproc.Projects.class);
        final Dataproc.Projects.Regions regions = mock(Dataproc.Projects.Regions.class);
        clusters = mock(Dataproc.Projects.Regions.Clusters.class);
        jobs = mock(Dataproc.Projects.Regions.Jobs.class);

        final Dataproc.Projects.Regions.Clusters.Get getClusterRequest = mock(Dataproc.Projects.Regions.Clusters.Get.class);
        getJobsRequest = mock(Dataproc.Projects.Regions.Jobs.Get.class);

        when(dataproc.projects()).thenReturn(projects);
        when(projects.regions()).thenReturn(regions);
        when(regions.clusters()).thenReturn(clusters);
        when(regions.jobs()).thenReturn(jobs);

        NodeInitialization nodeInitialization = mock(NodeInitialization.class);
        when(clusters.get(PROJECT, REGION, JOB_ID_AND_CLUSTER_NAME)).thenReturn(getClusterRequest);
        when(jobs.get(PROJECT, REGION, JOB_ID_AND_CLUSTER_NAME)).thenReturn(getJobsRequest);
        runtimeBucket = MockRuntimeBucket.of("sample").getRuntimeBucket();
        victim = new GoogleDataproc(dataproc, nodeInitialization, ARGUMENTS);
    }

    @Test
    public void onNewJobStartsCluster() throws Exception {
        setupJobSubmitMocks();
        victim.submit(runtimeBucket, JOB_DEFINITION);
        verify(clusters, times(1)).create(any(), any(), any());
    }

    @Test
    public void onNewJobSubmitsJob() throws Exception {
        setupJobSubmitMocks();
        victim.submit(runtimeBucket, JOB_DEFINITION);
        verify(clusters, times(1)).create(any(), any(), any());
        SubmitJobRequest value = submitRequestCaptor.getValue();
        assertThat(value.getJob().getReference().getJobId()).isEqualTo(JOB_ID_AND_CLUSTER_NAME);
    }

    @Test
    public void onNewJobCompletionDeletesCluster() throws Exception {
        setupJobSubmitMocks();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("RUNNING").thenReturn("RUNNING").thenReturn("DONE");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID_AND_CLUSTER_NAME))
                .setStatus(status));
        victim.submit(runtimeBucket, JOB_DEFINITION);
        verify(clusters, times(1)).delete(PROJECT, REGION, JOB_ID_AND_CLUSTER_NAME);
    }

    @Test
    public void onExistingButIncompleteJobDoesNotResubmitButWaitsForCompletion() throws Exception {
        setupJobSubmitMocks();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("RUNNING").thenReturn("RUNNING").thenReturn("DONE");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID_AND_CLUSTER_NAME))
                .setStatus(status));
        victim.submit(runtimeBucket, JOB_DEFINITION);
        verify(jobs, never()).submit(any(), any(), any());
    }

    @Test
    public void onExistingButFailedJobsDeletesAndResubmits() throws Exception {
        setupJobSubmitMocks();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("CANCELLED");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID_AND_CLUSTER_NAME))
                .setStatus(status));
        when(jobs.delete(PROJECT, REGION, JOB_ID_AND_CLUSTER_NAME)).thenReturn(mock(Dataproc.Projects.Regions.Jobs.Delete.class));
        victim.submit(runtimeBucket, JOB_DEFINITION);
        SubmitJobRequest value = submitRequestCaptor.getValue();
        assertThat(value.getJob().getReference().getJobId()).isEqualTo(JOB_ID_AND_CLUSTER_NAME);
    }

    private void setupJobSubmitMocks() throws java.io.IOException {
        Operation operation = mock(Operation.class);
        when(operation.getDone()).thenReturn(true);
        when(operation.getMetadata()).thenReturn(ImmutableMap.of("description", "none"));
        Dataproc.Projects.Regions.Clusters.Create create = mock(Dataproc.Projects.Regions.Clusters.Create.class);
        when(create.execute()).thenReturn(operation);
        when(clusters.create(any(), any(), any())).thenReturn(create);
        Dataproc.Projects.Regions.Jobs.Submit submit = mock(Dataproc.Projects.Regions.Jobs.Submit.class);
        submitRequestCaptor = ArgumentCaptor.forClass(SubmitJobRequest.class);
        when(jobs.submit(eq(PROJECT), eq(REGION), submitRequestCaptor.capture())).thenReturn(submit);
        when(submit.execute()).thenReturn(new Job().setStatus(new JobStatus().setState("DONE")));
        Dataproc.Projects.Regions.Clusters.Delete delete = mock(Dataproc.Projects.Regions.Clusters.Delete.class);
        when(delete.execute()).thenReturn(new Operation().setDone(true));
        when(clusters.delete(PROJECT, REGION, JOB_ID_AND_CLUSTER_NAME)).thenReturn(delete);
    }
}