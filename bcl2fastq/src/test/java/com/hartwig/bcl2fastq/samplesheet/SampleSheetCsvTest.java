package com.hartwig.bcl2fastq.samplesheet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.testsupport.Resources;

import org.junit.Test;

public class SampleSheetCsvTest {

    @Test(expected = IllegalArgumentException.class)
    public void noSampleSheetThrowsIllegalArgument() {
        Bucket inputBucket = mock(Bucket.class);
        SampleSheetCsv victim = new SampleSheetCsv(inputBucket, "test");
        victim.read().samples();
    }

    @Test
    public void parsesSampleSheet() throws Exception {
        Bucket inputBucket = mock(Bucket.class);
        Blob blob = mock(Blob.class);
        when(inputBucket.get("test/SampleSheet.csv")).thenReturn(blob);
        when(blob.getContent()).thenReturn(new FileInputStream(Resources.testResource("SampleSheet.csv")).readAllBytes());
        SampleSheetCsv victim = new SampleSheetCsv(inputBucket, "test");
        assertThat(victim.read().experimentName()).isEqualTo("IS19-0016");
        assertThat(victim.read().samples()).hasSize(8);
        assertThat(victim.read().samples().stream()).containsExactlyInAnyOrder(IlluminaSample.builder()
                        .barcode("FR17453070")
                        .sample("GIAB12878")
                        .project("HMFregVAL")
                        .build(),
                IlluminaSample.builder().barcode("FR17453071").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453072").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453073").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453074").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453075").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453076").sample("GIAB12878").project("HMFregVAL").build(),
                IlluminaSample.builder().barcode("FR17453077").sample("GIAB12878").project("HMFregVAL2").build());
    }
}