package com.hartwig.pipeline.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RunLogComponentTest {

    private static final String REPORT_BUCKET = "report_bucket";

    @Test
    public void copiesRunLogIntoReportBucket() {
        Storage storage = mock(Storage.class);
        RuntimeBucket runtimeBucket = MockRuntimeBucket.test().getRuntimeBucket();
        Bucket reportBucket = mock(Bucket.class);
        when(reportBucket.getName()).thenReturn(REPORT_BUCKET);
        ArgumentCaptor<String> sourceBlobCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBlobCaptor = ArgumentCaptor.forClass(String.class);
        RunLogComponent victim = new RunLogComponent(runtimeBucket,
                "test",
                Folder.from(TestInputs.referenceRunMetadata()),
                ResultsDirectory.defaultDirectory());
        victim.addToReport(storage, reportBucket, "test_set");
        verify(runtimeBucket, times(1)).copyOutOf(sourceBlobCaptor.capture(), targetBucketCaptor.capture(), targetBlobCaptor.capture());
        assertThat(sourceBlobCaptor.getValue()).isEqualTo("results/run.log");
        assertThat(targetBucketCaptor.getValue()).isEqualTo(REPORT_BUCKET);
        assertThat(targetBlobCaptor.getValue()).isEqualTo("test_set/reference/test/run.log");
    }
}