package com.hartwig.pipeline.cleanup;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;
import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.JobReference;
import com.google.api.services.dataproc.v1beta2.model.ListJobsResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CleanupTest {

    public static final ImmutableArguments ARGUMENTS = Arguments.testDefaultsBuilder().cleanup(true).build();
    private static final String REFERENCE_GUNZIP = "run-reference-test-gunzip";
    private static final String TUMOR_GUNZIP = "run-tumor-test-gunzip";
    private static final String RUN_REFERENCE = "run-reference-test";
    private static final String RUN_TUMOR = "run-tumor-test";
    private Storage storage;
    private Bucket referenceBucket;
    private Cleanup victim;
    private Bucket tumorBucket;
    private Bucket somaticBucket;
    private ListJobsResponse listJobsResponse;
    private Dataproc.Projects.Regions.Jobs jobs;
    private Dataproc dataproc;
    private SomaticMetadataApi somaticMetadataApi;
    private Bucket stagingBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        referenceBucket = mock(Bucket.class);
        tumorBucket = mock(Bucket.class);
        somaticBucket = mock(Bucket.class);
        dataproc = mock(Dataproc.class);
        Dataproc.Projects projects = mock(Dataproc.Projects.class);
        Dataproc.Projects.Regions regions = mock(Dataproc.Projects.Regions.class);
        jobs = mock(Dataproc.Projects.Regions.Jobs.class);
        when(dataproc.projects()).thenReturn(projects);
        when(projects.regions()).thenReturn(regions);
        when(regions.jobs()).thenReturn(jobs);
        Dataproc.Projects.Regions.Jobs.List list = mock(Dataproc.Projects.Regions.Jobs.List.class);
        when(jobs.list(ARGUMENTS.project(), ARGUMENTS.region())).thenReturn(list);
        listJobsResponse = mock(ListJobsResponse.class);
        when(list.execute()).thenReturn(listJobsResponse);

        stagingBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(stagingBucket);
        Page<Blob> page = TestBlobs.pageOf();
        when(stagingBucket.list(any())).thenReturn(page);

        somaticMetadataApi = mock(SomaticMetadataApi.class);
        victim = new Cleanup(storage, ARGUMENTS, dataproc, somaticMetadataApi);
    }

    @NotNull
    private Job job(final String jobId) {
        return new Job().setReference(new JobReference().setJobId(jobId));
    }

    @Test
    public void doesNothingWhenCleanupDisabled() {
        victim = new Cleanup(storage, Arguments.testDefaultsBuilder().cleanup(false).build(), dataproc, somaticMetadataApi);
        victim.run(defaultSomaticRunMetadata());
        verify(referenceBucket, never()).delete();
    }

    @Test
    public void deletesReferenceBucketIfExists() {
        assertBucketDeleted(RUN_REFERENCE, referenceBucket);
    }

    @Test
    public void deletesTumorBucketIfExists() {
        assertBucketDeleted(RUN_TUMOR, tumorBucket);
    }

    @Test
    public void deletesSomaticBucketIfExists() {
        assertBucketDeleted("run-reference-tumor-test", somaticBucket);
    }

    @Test
    public void deletesAllDataprocJobsMatchingRunIds() throws Exception {
        when(listJobsResponse.getJobs()).thenReturn(Lists.newArrayList(job(REFERENCE_GUNZIP),
                job(TUMOR_GUNZIP),
                job("run-something-else")));
        ArgumentCaptor<String> deletedJobs = ArgumentCaptor.forClass(String.class);
        Dataproc.Projects.Regions.Jobs.Delete delete = mock(Dataproc.Projects.Regions.Jobs.Delete.class);
        when(jobs.delete(eq(ARGUMENTS.project()), eq(ARGUMENTS.region()), deletedJobs.capture())).thenReturn(delete);
        when(jobs.get(eq(ARGUMENTS.project()), eq(ARGUMENTS.region()), any())).thenThrow(GoogleJsonResponseException.class);
        victim.run(defaultSomaticRunMetadata());
        assertThat(deletedJobs.getAllValues()).hasSize(2);
        assertThat(deletedJobs.getAllValues().get(0)).isEqualTo(REFERENCE_GUNZIP);
        assertThat(deletedJobs.getAllValues().get(1)).isEqualTo(TUMOR_GUNZIP);
    }

    @Test
    public void deletesStagingBucketIfExists() {
        Blob output = TestBlobs.blob("output.txt");
        Page<Blob> page = TestBlobs.pageOf(output);
        when(stagingBucket.list(Storage.BlobListOption.prefix("reference-test"))).thenReturn(page);
        victim.run(defaultSomaticRunMetadata());
        verify(output, times(1)).delete();
    }

    @Test
    public void retriesDeletingDataprocJobsIfTheyStillExist() throws Exception {
        Job job = job(REFERENCE_GUNZIP);
        when(listJobsResponse.getJobs()).thenReturn(Lists.newArrayList(job));
        Dataproc.Projects.Regions.Jobs.Delete delete = mock(Dataproc.Projects.Regions.Jobs.Delete.class);
        when(jobs.delete(ARGUMENTS.project(), ARGUMENTS.region(), job.getReference().getJobId())).thenReturn(delete);
        Dataproc.Projects.Regions.Jobs.Get get = mock(Dataproc.Projects.Regions.Jobs.Get.class);
        when(get.execute()).thenReturn(job).thenThrow(GoogleJsonResponseException.class);
        when(jobs.get(ARGUMENTS.project(), ARGUMENTS.region(), job.getReference().getJobId())).thenReturn(get);
        victim.run(defaultSomaticRunMetadata());
        verify(delete, times(2)).execute();
    }

    private void assertBucketDeleted(final String bucketName, final Bucket bucket) {
        Blob blob = returnBlob(bucketName, bucket);
        victim.run(defaultSomaticRunMetadata());
        verify(bucket, times(1)).delete();
        verify(blob, times(1)).delete();
    }

    private Blob returnBlob(final String bucketName, final Bucket bucket) {
        when(storage.get(bucketName)).thenReturn(bucket);
        Blob blob = blob("result");
        Page<Blob> page = pageOf(blob);
        when(bucket.list()).thenReturn(page);
        return blob;
    }
}