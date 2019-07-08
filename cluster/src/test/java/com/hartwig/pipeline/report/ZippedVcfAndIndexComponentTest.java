package com.hartwig.pipeline.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ZippedVcfAndIndexComponentTest {

    private static final String OUTPUT_BUCKET = "output";
    private static final String TEST_VCF = "test.vcf";

    @Test
    public void copiesVcfAndTbiToOutpput() {
        Storage storage = mock(Storage.class);
        RuntimeBucket runtimeBucket = MockRuntimeBucket.test().getRuntimeBucket();
        Bucket reportBucket = mock(Bucket.class);
        when(reportBucket.getName()).thenReturn(OUTPUT_BUCKET);
        ArgumentCaptor<String> sourceBlobCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> targetBlobCaptor = ArgumentCaptor.forClass(String.class);
        ZippedVcfAndIndexComponent victim = new ZippedVcfAndIndexComponent(runtimeBucket,
                "test",
                Folder.from(TestInputs.referenceRunMetadata()), TEST_VCF, TEST_VCF,
                ResultsDirectory.defaultDirectory());
        victim.addToReport(storage, reportBucket, "test_set");
        verify(runtimeBucket, times(2)).copyOutOf(sourceBlobCaptor.capture(), targetBucketCaptor.capture(), targetBlobCaptor.capture());
        assertThat(sourceBlobCaptor.getAllValues().get(0)).isEqualTo("results/test.vcf");
        assertThat(sourceBlobCaptor.getAllValues().get(1)).isEqualTo("results/test.vcf.tbi");
        assertThat(targetBucketCaptor.getValue()).isEqualTo(OUTPUT_BUCKET);
        assertThat(targetBlobCaptor.getAllValues().get(0)).isEqualTo("test_set/reference/test/test.vcf");
        assertThat(targetBlobCaptor.getAllValues().get(1)).isEqualTo("test_set/reference/test/test.vcf.tbi");
    }

}