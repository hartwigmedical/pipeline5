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
import java.util.stream.Collectors;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.io.ResultsDirectory;
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
        victim = new DataprocLogComponent(TestSamples.simpleReferenceSample(), runtimeBucket, ResultsDirectory.defaultDirectory());
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
        String gunzipPath1 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-gunzip/driveroutput.000";
        String gunzipPath2 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-gunzip/driveroutput.001";
        String sortPath1 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-sortandindex/driveroutput.000";
        String sortPath2 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-sortandindex/driveroutput.001";
        List<Blob> blobs = Lists.newArrayList(TestBlobs.blob(gunzipPath1),
                TestBlobs.blob(gunzipPath2),
                TestBlobs.blob(sortPath1),
                TestBlobs.blob(sortPath2),
                TestBlobs.blob(DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-sortandindex/another.file"));
        when(page.iterateAll()).thenReturn(blobs);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(REPORT_BUCKET);
        ArgumentCaptor<Storage.ComposeRequest> composeCaptor = ArgumentCaptor.forClass(Storage.ComposeRequest.class);
        ArgumentCaptor<Storage.CopyRequest> copyCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        victim.addToReport(storage, bucket, TEST_SET);
        verify(storage, times(2)).compose(composeCaptor.capture());
        verify(storage, times(2)).copy(copyCaptor.capture());
        assertThat(composeCaptor.getAllValues()).hasSize(2);
        assertThat(copyCaptor.getAllValues()).hasSize(2);
        checkCompose(composeCaptor, "aligner/results/sortandindex-run.log", 0, sortPath1, sortPath2);
        checkCompose(composeCaptor, "aligner/results/gunzip-run.log", 1, gunzipPath1, gunzipPath2);
        checkCopy(copyCaptor, 0, "sortandindex");
        checkCopy(copyCaptor, 1, "gunzip");
    }

    private void checkCopy(final ArgumentCaptor<Storage.CopyRequest> copyCaptor, final int index, final String stepName) {
        Storage.CopyRequest copyRequest = copyCaptor.getAllValues().get(index);
        assertThat(copyRequest.getSource().getBucket()).isEqualTo(runtimeBucket.runId());
        assertThat(copyRequest.getSource().getName()).isEqualTo(format("aligner/results/%s-run.log", stepName));
        assertThat(copyRequest.getTarget().getBucket()).isEqualTo(REPORT_BUCKET);
        assertThat(copyRequest.getTarget().getName()).isEqualTo(format("test-set/sample/aligner/working/%s-run.log", stepName));
    }

    @Test
    public void composesLogsAlwaysInLexographicOrder() {
        String gunzipPath1 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-gunzip/driveroutput.000";
        String gunzipPath2 = DataprocLogComponent.METADATA_PATH + "/xyz/jobs/run-test-gunzip/driveroutput.001";
        List<Blob> blobs = Lists.newArrayList(TestBlobs.blob(gunzipPath2), TestBlobs.blob(gunzipPath1));
        when(page.iterateAll()).thenReturn(blobs);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(REPORT_BUCKET);
        ArgumentCaptor<Storage.ComposeRequest> composeCaptor = ArgumentCaptor.forClass(Storage.ComposeRequest.class);
        victim.addToReport(storage, bucket, TEST_SET);
        verify(storage, times(1)).compose(composeCaptor.capture());
        assertThat(composeCaptor.getAllValues()).hasSize(1);
        assertThat(composeCaptor.getValue().getSourceBlobs().get(0).getName()).isEqualTo(gunzipPath1);
        assertThat(composeCaptor.getValue().getSourceBlobs().get(1).getName()).isEqualTo(gunzipPath2);
    }

    private void checkCompose(final ArgumentCaptor<Storage.ComposeRequest> composeCaptor, final String target, final int index,
            String... paths) {
        Storage.ComposeRequest composeRequest = composeCaptor.getAllValues().get(index);
        assertThat(composeRequest.getSourceBlobs()
                .stream()
                .map(Storage.ComposeRequest.SourceBlob::getName)
                .collect(Collectors.toList())).containsExactlyInAnyOrder(paths);
        assertThat(composeRequest.getTarget().getName()).isEqualTo(target);
    }
}