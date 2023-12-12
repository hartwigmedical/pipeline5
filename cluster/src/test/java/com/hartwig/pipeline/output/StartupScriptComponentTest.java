package com.hartwig.pipeline.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StartupScriptComponentTest {

    private static final String REPORT_BUCKET = "report_bucket";

    @Test
    public void copiesStartupScriptIntoReportBucket() {
        Storage storage = mock(Storage.class);
        RuntimeBucket runtimeBucket = MockRuntimeBucket.test().getRuntimeBucket();
        Bucket reportBucket = mock(Bucket.class);
        when(reportBucket.getName()).thenReturn(REPORT_BUCKET);
        ArgumentCaptor<String> sourceBlobCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBlobCaptor = ArgumentCaptor.forClass(String.class);
        StartupScriptComponent victim = new StartupScriptComponent(runtimeBucket,
                "test",
                Folder.from(TestInputs.referenceRunMetadata()));
        victim.addToOutput(storage, reportBucket, "test_set");
        verify(runtimeBucket, times(1)).copyOutOf(sourceBlobCaptor.capture(), targetBucketCaptor.capture(), targetBlobCaptor.capture());
        assertThat(sourceBlobCaptor.getValue()).isEqualTo("copy_of_startup_script_for_run.sh");
        assertThat(targetBucketCaptor.getValue()).isEqualTo(REPORT_BUCKET);
        assertThat(targetBlobCaptor.getValue()).isEqualTo("test_set/reference/test/run.sh");
    }
}