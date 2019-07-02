package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DataprocLogComponentTest {

    private static final String REPORT_BUCKET = "test-reports";
    private static final String TEST_SET = "test-set";
    private DataprocLogComponent victim;
    private Storage storage;
    private Page<Blob> page;
    private RuntimeBucket runtimeBucket;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        runtimeBucket = MockRuntimeBucket.test().getRuntimeBucket();
        storage = mock(Storage.class);
        page = mock(Page.class);
        when(storage.list(runtimeBucket.runId(), Storage.BlobListOption.prefix(DataprocLogComponent.METADATA_PATH))).thenReturn(page);
        victim = new DataprocLogComponent(TestSamples.simpleReferenceSample(), runtimeBucket);
    }

    @Test
    public void doesNothingWhenNoDataprocMetadataFound() {
        List<Blob> blobArrayList = Lists.newArrayList(TestBlobs.blob("not/dataproc.log"));
        when(page.iterateAll()).thenReturn(blobArrayList);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(REPORT_BUCKET);
        victim.addToReport(storage, bucket, TEST_SET);
        verify(storage, never()).copy(any());
    }

    @Test
    public void noErrorsWhenPathsAreNotOfExpectedConvention() {
        List<Blob> blobs = Lists.newArrayList(TestBlobs.blob(DataprocLogComponent.METADATA_PATH + "/run-test-gunzip/driveroutput.000"),
                TestBlobs.blob(DataprocLogComponent.METADATA_PATH + "/run-test-sortandindex/driveroutput.000"),
                TestBlobs.blob(DataprocLogComponent.METADATA_PATH + "/run-test-sortandindex/another.file"));
        when(page.iterateAll()).thenReturn(blobs);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(REPORT_BUCKET);
        victim.addToReport(storage, bucket, TEST_SET);
        verify(storage, never()).copy(any());
    }

    @Test
    public void composesLogsByStepName() {
        String gunzipPath = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-gunzip/driveroutput.000000000";
        String sortPath = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-sortandindex/driveroutput.000000000";
        List<Blob> blobs = Lists.newArrayList(TestBlobs.blob(gunzipPath),
                TestBlobs.blob(sortPath),
                TestBlobs.blob(DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-sortandindex/another.file"));
        when(page.iterateAll()).thenReturn(blobs);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(REPORT_BUCKET);
        ArgumentCaptor<Storage.CopyRequest> copyCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        victim.addToReport(storage, bucket, TEST_SET);
        verify(storage, times(2)).copy(copyCaptor.capture());
        assertThat(copyCaptor.getAllValues()).hasSize(2);
        checkCopy(copyCaptor, 0, "sortandindex");
        checkCopy(copyCaptor, 1, "gunzip");
    }

    private void checkCopy(final ArgumentCaptor<Storage.CopyRequest> copyCaptor, final int index, final String stepName) {
        Storage.CopyRequest copyRequest = copyCaptor.getAllValues().get(index);
        assertThat(copyRequest.getSource().getBucket()).isEqualTo(runtimeBucket.runId());
        assertThat(copyRequest.getSource().getName()).isEqualTo(format(
                "google-cloud-dataproc-metainfo/xyz/jobs/run-test-%s/driveroutput.000000000",
                stepName));
        assertThat(copyRequest.getTarget().getBucket()).isEqualTo(REPORT_BUCKET);
        assertThat(copyRequest.getTarget().getName()).isEqualTo(format("test-set/sample/aligner/%s_run.log", stepName));
    }
}