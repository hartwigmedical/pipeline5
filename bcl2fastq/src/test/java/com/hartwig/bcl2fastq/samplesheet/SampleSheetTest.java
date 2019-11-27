package com.hartwig.bcl2fastq.samplesheet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.testsupport.Resources;

import org.junit.Test;

public class SampleSheetTest {

    @Test(expected = IllegalArgumentException.class)
    public void noSampleSheetThrowsIllegalArgument() {
        Bucket inputBucket = mock(Bucket.class);
        SampleSheet victim = new SampleSheet(inputBucket, "test");
        victim.projects();
    }

    @Test
    public void parsesSampleSheet() throws Exception {
        Bucket inputBucket = mock(Bucket.class);
        Blob blob = mock(Blob.class);
        when(inputBucket.get("test/SampleSheet.csv")).thenReturn(blob);
        when(blob.getContent()).thenReturn(new FileInputStream(Resources.testResource("SampleSheet.csv")).readAllBytes());
        SampleSheet victim = new SampleSheet(inputBucket, "test");
        assertThat(victim.projects()).hasSize(2);
        assertThat(victim.projects()).containsExactlyInAnyOrder("HMFregVAL2", "HMFregVAL");
    }
}