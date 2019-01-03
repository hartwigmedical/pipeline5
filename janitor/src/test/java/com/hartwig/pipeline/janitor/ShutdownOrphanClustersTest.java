package com.hartwig.pipeline.janitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.ClusterStatus;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.ListClustersResponse;
import com.google.api.services.dataproc.model.ListJobsResponse;
import com.google.api.services.dataproc.model.Operation;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

public class ShutdownOrphanClustersTest {

    private static final String PROJECT = "project";
    private static final String REGION = "region";
    private static final Arguments ARGUMENTS =
            Arguments.builder().project(PROJECT).region(REGION).privateKeyPath("privatekey").intervalInSeconds(10).build();
    private Dataproc.Projects.Regions.Clusters clusters;
    private ListClustersResponse listClusterResponse;
    private ShutdownOrphanClusters victim;
    private ListJobsResponse listJobsResponse;

    @Before
    public void setUp() throws Exception {
        final Dataproc dataproc = mock(Dataproc.class);
        final Dataproc.Projects projects = mock(Dataproc.Projects.class);
        final Dataproc.Projects.Regions regions = mock(Dataproc.Projects.Regions.class);
        clusters = mock(Dataproc.Projects.Regions.Clusters.class);
        final Dataproc.Projects.Regions.Jobs jobs = mock(Dataproc.Projects.Regions.Jobs.class);

        final Dataproc.Projects.Regions.Clusters.List listClusterRequest = mock(Dataproc.Projects.Regions.Clusters.List.class);
        listClusterResponse = mock(ListClustersResponse.class);

        Dataproc.Projects.Regions.Jobs.List listJobsRequest = mock(Dataproc.Projects.Regions.Jobs.List.class);
        listJobsResponse = mock(ListJobsResponse.class);

        when(dataproc.projects()).thenReturn(projects);
        when(projects.regions()).thenReturn(regions);
        when(regions.clusters()).thenReturn(clusters);
        when(regions.jobs()).thenReturn(jobs);

        when(clusters.list(PROJECT, REGION)).thenReturn(listClusterRequest);
        when(listClusterRequest.execute()).thenReturn(listClusterResponse);

        when(jobs.list(PROJECT, REGION)).thenReturn(listJobsRequest);
        when(listJobsRequest.execute()).thenReturn(listJobsResponse);

        victim = new ShutdownOrphanClusters(dataproc);
    }

    @Test(expected = RuntimeException.class)
    public void wrapsAndRethrowsIOExceptionAsRuntimeException() throws Exception {
        when(clusters.list(PROJECT, REGION)).thenThrow(new IOException());
        victim.execute(ARGUMENTS);
    }

    @Test
    public void findsRunningJobsInEachCluster() {
        Cluster cluster1 = cluster("cluster1");
        Cluster cluster2 = cluster("cluster2");
        Job job1 = job("job1", cluster1.getClusterName());
        Job job2 = job("job2", cluster2.getClusterName());
        Job job3 = job("job3", cluster1.getClusterName());
        Job job4 = job("done", cluster1.getClusterName(), "Done");

        returnClusters(cluster1, cluster2);
        returnJobs(job1, job2, job3, job4);
        victim.execute(ARGUMENTS);

        Map<Cluster, List<Job>> runningClusters = victim.getRunningClusters();
        assertThat(runningClusters.get(cluster1)).containsOnly(job1, job3);
        assertThat(runningClusters.get(cluster2)).containsOnly(job2);
    }

    @Test
    public void shutsDownAllClustersWithNoRunningJobs() throws Exception {
        Cluster runningJobs = cluster("running");
        Cluster noRunningJobs = cluster("orphan");

        returnClusters(runningJobs, noRunningJobs);
        returnJobs(job("job1", runningJobs.getClusterName()));

        Dataproc.Projects.Regions.Clusters.Delete delete = mock(Dataproc.Projects.Regions.Clusters.Delete.class);
        Operation operation = new Operation().setName("delete").setDone(true).setMetadata(ImmutableMap.of("description", "none"));
        when(delete.execute()).thenReturn(operation);
        when(clusters.delete(PROJECT, REGION, "orphan")).thenReturn(delete);

        victim.execute(ARGUMENTS);
        verify(clusters, times(1)).delete(PROJECT, REGION, "orphan");
    }

    private Job job(final String name, final String cluster) {
        return job(name, cluster, "RUNNING");
    }

    private Job job(final String name, final String cluster, final String status) {
        return new Job().setReference(new JobReference().setJobId(name))
                .setPlacement(new JobPlacement().setClusterName(cluster))
                .setStatus(new JobStatus().setState(status));
    }

    private static Cluster cluster(final String name) {
        return new Cluster().setClusterName(name).setStatus(new ClusterStatus().setState("RUNNING"));
    }

    private void returnJobs(final Job... jobs) {
        when(listJobsResponse.getJobs()).thenReturn(Arrays.asList(jobs));
    }

    private void returnClusters(final Cluster... clusters) {
        when(listClusterResponse.getClusters()).thenReturn(Arrays.asList(clusters));
    }
}