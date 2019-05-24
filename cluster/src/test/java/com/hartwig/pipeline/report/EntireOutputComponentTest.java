package com.hartwig.pipeline.report;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class EntireOutputComponentTest {

    private static final String REPORT_BUCKET = "report_bucket";
    private Storage storage;
    private RuntimeBucket runtimeBucket;
    private ArgumentCaptor<String> sourceBlobCaptor;
    private ArgumentCaptor<String> targetBucketCaptor;
    private ArgumentCaptor<String> targetBlobCaptor;
    private Bucket reportBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        runtimeBucket = MockRuntimeBucket.test().getRuntimeBucket();
        reportBucket = mock(Bucket.class);
        when(reportBucket.getName()).thenReturn(REPORT_BUCKET);
        sourceBlobCaptor = ArgumentCaptor.forClass(String.class);
        targetBucketCaptor = ArgumentCaptor.forClass(String.class);
        targetBlobCaptor = ArgumentCaptor.forClass(String.class);
    }

    @Test
    public void copiesRunLogIntoReportBucket() {

        Blob first = blob("results/file1.out");
        Blob second = blob("results/file2.out");
        when(runtimeBucket.list("results")).thenReturn(Lists.newArrayList(first, second));

        EntireOutputComponent victim =
                new EntireOutputComponent(runtimeBucket, TestInputs.defaultPair(), "namespace", ResultsDirectory.defaultDirectory());
        victim.addToReport(storage, reportBucket, "test_set");
        verify(runtimeBucket, times(2)).copyOutOf(sourceBlobCaptor.capture(), targetBucketCaptor.capture(), targetBlobCaptor.capture());
        assertThat(sourceBlobCaptor.getAllValues().get(0)).isEqualTo("results/file1.out");
        assertThat(targetBucketCaptor.getAllValues().get(0)).isEqualTo(REPORT_BUCKET);
        assertThat(targetBlobCaptor.getAllValues().get(0)).isEqualTo("test_set/reference_tumor/namespace/file1.out");
        assertThat(sourceBlobCaptor.getAllValues().get(1)).isEqualTo("results/file2.out");
        assertThat(targetBucketCaptor.getAllValues().get(1)).isEqualTo(REPORT_BUCKET);
        assertThat(targetBlobCaptor.getAllValues().get(1)).isEqualTo("test_set/reference_tumor/namespace/file2.out");
    }

    @Test
    public void ignoresExcludedFiles() {

        Blob first = blob("results/file1.out");
        String excludedFileName = "results/file2.out";
        Blob excluded = blob(excludedFileName);
        when(runtimeBucket.list("results")).thenReturn(Lists.newArrayList(first, excluded));

        EntireOutputComponent victim = new EntireOutputComponent(runtimeBucket,
                TestInputs.defaultPair(),
                "namespace",
                ResultsDirectory.defaultDirectory(),
                excludedFileName);
        victim.addToReport(storage, reportBucket, "test_set");
        verify(runtimeBucket, times(1)).copyOutOf(sourceBlobCaptor.capture(), targetBucketCaptor.capture(), targetBlobCaptor.capture());
        assertThat(sourceBlobCaptor.getAllValues().get(0)).isEqualTo("results/file1.out");
        assertThat(targetBucketCaptor.getAllValues().get(0)).isEqualTo(REPORT_BUCKET);
        assertThat(targetBlobCaptor.getAllValues().get(0)).isEqualTo("test_set/reference_tumor/namespace/file1.out");
    }
}