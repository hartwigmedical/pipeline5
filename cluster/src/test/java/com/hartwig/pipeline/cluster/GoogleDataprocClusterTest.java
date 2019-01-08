package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.Operation;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.common.collect.ImmutableMap;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GoogleDataprocClusterTest {

    private static final String REGION = "region";
    private static final String PROJECT = "project";
    private static final Arguments ARGUMENTS = Arguments.defaultsBuilder().project(PROJECT).region(REGION).build();
    private static final String CLUSTER_NAME = "sample-cluster";
    private static final String JOB_ID = "sample-cluster-gunzip";
    private Dataproc.Projects.Regions.Clusters clusters;
    private Dataproc.Projects.Regions.Jobs jobs;
    private GoogleDataprocCluster victim;
    private Dataproc.Projects.Regions.Clusters.Get getClusterRequest;
    private Dataproc.Projects.Regions.Jobs.Get getJobsRequest;

    @Before
    public void setUp() throws Exception {
        final Dataproc dataproc = mock(Dataproc.class);
        final Dataproc.Projects projects = mock(Dataproc.Projects.class);
        final Dataproc.Projects.Regions regions = mock(Dataproc.Projects.Regions.class);
        clusters = mock(Dataproc.Projects.Regions.Clusters.class);
        jobs = mock(Dataproc.Projects.Regions.Jobs.class);

        getClusterRequest = mock(Dataproc.Projects.Regions.Clusters.Get.class);
        getJobsRequest = mock(Dataproc.Projects.Regions.Jobs.Get.class);

        when(dataproc.projects()).thenReturn(projects);
        when(projects.regions()).thenReturn(regions);
        when(regions.clusters()).thenReturn(clusters);
        when(regions.jobs()).thenReturn(jobs);

        NodeInitialization nodeInitialization = mock(NodeInitialization.class);
        when(clusters.get(PROJECT, REGION, CLUSTER_NAME)).thenReturn(getClusterRequest);
        when(jobs.get(PROJECT, REGION, JOB_ID)).thenReturn(getJobsRequest);
        victim = new GoogleDataprocCluster(dataproc, nodeInitialization, "cluster");
    }

    @Test
    public void doesNotStartClusterIfAlreadyExists() throws Exception {
        startClusterWithExisting();
        verify(clusters, never()).create(any(), any(), any());
        assertThat(victim.isStarted()).isTrue();
    }

    @Test
    public void startsClusterAndWaitsForRunning() throws Exception {
        Operation operation = mock(Operation.class);
        when(operation.getDone()).thenReturn(true);
        when(operation.getMetadata()).thenReturn(ImmutableMap.of("description", "none"));
        Dataproc.Projects.Regions.Clusters.Create create = mock(Dataproc.Projects.Regions.Clusters.Create.class);
        when(create.execute()).thenReturn(operation);
        when(clusters.create(any(), any(), any())).thenReturn(create);
        victim.start(PerformanceProfile.mini(),
                Sample.builder("", "sample").build(),
                MockRuntimeBucket.of("sample").getRuntimeBucket(),
                ARGUMENTS);
        verify(clusters, times(1)).create(any(), any(), any());
        assertThat(victim.isStarted()).isTrue();
    }

    @Test
    public void doesNotSubmitJobIfExistsAndRunningButDoesWaitForCompletion() throws Exception {
        startClusterWithExisting();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("RUNNING").thenReturn("RUNNING").thenReturn("DONE");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID)).setStatus(status));
        victim.submit(SparkJobDefinition.gunzip(JarLocation.of("jar"), PerformanceProfile.mini()), ARGUMENTS);
        verify(jobs, never()).submit(any(), any(), any());
    }

    @Test
    public void resubmitsJobIfExistsButNotRunningOrDone() throws Exception {
        startClusterWithExisting();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("CANCELLED");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID)).setStatus(status));
        Dataproc.Projects.Regions.Jobs.Submit submit = mock(Dataproc.Projects.Regions.Jobs.Submit.class);
        ArgumentCaptor<SubmitJobRequest> submitRequestCaptor = ArgumentCaptor.forClass(SubmitJobRequest.class);
        when(jobs.submit(eq(PROJECT), eq(REGION), submitRequestCaptor.capture())).thenReturn(submit);
        when(submit.execute()).thenReturn(new Job().setStatus(new JobStatus().setState("DONE")));
        when(jobs.delete(PROJECT, REGION, JOB_ID)).thenReturn(mock(Dataproc.Projects.Regions.Jobs.Delete.class));
        victim.submit(SparkJobDefinition.gunzip(JarLocation.of("jar"), PerformanceProfile.mini()), ARGUMENTS);
        SubmitJobRequest value = submitRequestCaptor.getValue();
        assertThat(value.getJob().getReference().getJobId()).isEqualTo(JOB_ID);
    }

    @Test
    public void skipsJobWhenJobExistsAndMarkedDone() throws Exception {
        startClusterWithExisting();
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn("DONE");
        when(getJobsRequest.execute()).thenReturn(new Job().setReference(new JobReference().setJobId(JOB_ID)).setStatus(status));
        victim.submit(SparkJobDefinition.gunzip(JarLocation.of("jar"), PerformanceProfile.mini()), ARGUMENTS);
        verify(jobs, never()).submit(any(), any(), any());
    }

    @Test
    public void doesNotSubmitJobIfClusterNotStarted() throws Exception {
        victim.submit(SparkJobDefinition.gunzip(JarLocation.of("jar"), PerformanceProfile.mini()), ARGUMENTS);
        verify(jobs, never()).submit(any(), any(), any());
    }

    @Test
    public void submitsJobAndWaitsForCompletion() throws Exception {
        startClusterWithExisting();
        Dataproc.Projects.Regions.Jobs.Submit submit = mock(Dataproc.Projects.Regions.Jobs.Submit.class);
        ArgumentCaptor<SubmitJobRequest> submitRequestCaptor = ArgumentCaptor.forClass(SubmitJobRequest.class);
        when(jobs.submit(eq(PROJECT), eq(REGION), submitRequestCaptor.capture())).thenReturn(submit);
        when(submit.execute()).thenReturn(new Job().setStatus(new JobStatus().setState("DONE")));
        victim.submit(SparkJobDefinition.gunzip(JarLocation.of("jar"), PerformanceProfile.mini()), ARGUMENTS);
        SubmitJobRequest value = submitRequestCaptor.getValue();
        assertThat(value.getJob().getReference().getJobId()).isEqualTo(JOB_ID);
    }

    @Test
    public void remoteClusterDeletedWhenStopped() throws Exception {
        startClusterWithExisting();
        Dataproc.Projects.Regions.Clusters.Delete delete = mock(Dataproc.Projects.Regions.Clusters.Delete.class);
        when(delete.execute()).thenReturn(new Operation().setDone(true));
        when(clusters.delete(PROJECT, REGION, CLUSTER_NAME)).thenReturn(delete);
        victim.stop(ARGUMENTS);
        verify(clusters, times(1)).delete(PROJECT, REGION, CLUSTER_NAME);
    }

    private void startClusterWithExisting() throws IOException {
        when(clusters.get(PROJECT, REGION, CLUSTER_NAME)).thenReturn(getClusterRequest);
        when(getClusterRequest.execute()).thenReturn(new Cluster().setClusterName(CLUSTER_NAME));
        victim.start(PerformanceProfile.mini(),
                Sample.builder("", "sample").build(),
                MockRuntimeBucket.of("sample").getRuntimeBucket(),
                ARGUMENTS);
    }
}