package com.hartwig.bcl2fastq.conversion;

import static com.hartwig.bcl2fastq.stats.TestStats.laneStats;
import static com.hartwig.bcl2fastq.stats.TestStats.sampleStats;
import static com.hartwig.pipeline.testsupport.TestBlobs.blob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.common.collect.Lists;
import com.hartwig.bcl2fastq.samplesheet.IlluminaSample;
import com.hartwig.bcl2fastq.samplesheet.ImmutableSampleSheet;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.stats.ImmutableLaneStats;
import com.hartwig.bcl2fastq.stats.ImmutableStats;
import com.hartwig.bcl2fastq.stats.TestStats;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class ResultAggregationTest {

    private static final String FLOWCELL = "flowcell";
    private static final String EXPERIMENT = "experiment";
    private static final String BARCODE = "barcode";
    private static final String PROJECT = "project";
    private static final String SAMPLE = "sample";
    private RuntimeBucket bucket;
    private ResultAggregation victim;
    private Blob first;
    private Blob second;
    private Blob third;
    private Blob fourth;
    private String path;
    public static final long SIZE_R1 = 5L;
    public static final long SIZE_R2 = 6L;
    public static final String MD5_R1 = "md51";
    public static final String MD5_R2 = "md52";

    @Before
    public void setUp() {
        bucket = mock(RuntimeBucket.class);
        victim = new ResultAggregation(bucket, ResultsDirectory.defaultDirectory());
        path = String.format("results/%s/%s", PROJECT, BARCODE);
        first = blob(path + "/GIAB12878_S1_L001_R1_001.fastq.gz");
        second = blob(path + "/GIAB12878_S1_L001_R2_001.fastq.gz");
        third = blob(path + "/GIAB12878_S1_L002_R1_001.fastq.gz");
        fourth = blob(path + "/GIAB12878_S1_L002_R2_001.fastq.gz");
        when(first.getSize()).thenReturn(SIZE_R1);
        when(first.getMd5()).thenReturn(MD5_R1);
        when(second.getSize()).thenReturn(SIZE_R2);
        when(second.getMd5()).thenReturn(MD5_R2);
        when(third.getSize()).thenReturn(SIZE_R1);
        when(third.getMd5()).thenReturn(MD5_R1);
        when(fourth.getSize()).thenReturn(SIZE_R2);
        when(fourth.getMd5()).thenReturn(MD5_R2);
    }

    @Test
    public void emptySampleSheetReturnsEmptyResults() {
        Conversion conversion = victim.apply(SampleSheet.builder().experimentName(EXPERIMENT).build(), defaultStats());
        assertThat(conversion.samples()).isEmpty();
        assertThat(conversion.flowcell()).isEqualTo(FLOWCELL);
    }

    @Test
    public void singleSamplePopulatedWithMetadata() {
        Conversion conversion = victim.apply(sampleSheet(), defaultStats());

        assertThat(conversion.samples()).hasSize(1);
        ConvertedSample sample = conversion.samples().get(0);
        assertThat(sample.barcode()).isEqualTo(BARCODE);
        assertThat(sample.sample()).isEqualTo(SAMPLE);
        assertThat(sample.project()).isEqualTo(PROJECT);
    }

    @Test
    public void singleSamplePopulatedWithFastQFilesInStorage() {

        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second, third, fourth));

        Conversion conversion = victim.apply(sampleSheet(),
                stats(laneStats(1, 1, sampleStats(BARCODE, 2, 1, 2)), laneStats(2, 1, sampleStats(BARCODE, 2, 1, 2))));

        assertThat(conversion.samples()).hasSize(1);
        List<ConvertedFastq> fastq = conversion.samples().get(0).fastq();
        assertThat(fastq).hasSize(2);
        ConvertedFastq firstFastq = fastq.get(0);
        assertThat(firstFastq.id()).isEqualTo(FastqId.of(1, BARCODE));
        assertThat(firstFastq.pathR1()).isEqualTo(first.getName());
        assertThat(firstFastq.pathR2()).hasValue(second.getName());
        ConvertedFastq secondFastq = fastq.get(1);
        assertThat(secondFastq.id()).isEqualTo(FastqId.of(2, BARCODE));
        assertThat(secondFastq.pathR1()).isEqualTo(third.getName());
        assertThat(secondFastq.pathR2()).hasValue(fourth.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void failsWhenStatsMissingForFastq() {

        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second, third, fourth));

        victim.apply(sampleSheet(), stats(laneStats(1, 1, sampleStats(BARCODE, 2, 1, 2))));
    }

    @Test
    public void fastqYieldAndQ30CalculatedFromStats() {

        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second));

        Conversion conversion = victim.apply(sampleSheet(), defaultStats());

        assertThat(conversion.samples()).hasSize(1);
        List<ConvertedFastq> fastq = conversion.samples().get(0).fastq();
        assertThat(fastq).hasSize(1);
        ConvertedFastq firstFastq = fastq.get(0);
        assertThat(firstFastq.id()).isEqualTo(FastqId.of(1, BARCODE));
        assertThat(firstFastq.yield()).isEqualTo(3);
        assertThat(firstFastq.yieldQ30()).isEqualTo(3);
    }

    @Test
    public void sizeAndMD5PopulatedFromGoogleCloudStorage() {
        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second));

        Conversion conversion = victim.apply(sampleSheet(), defaultStats());
        List<ConvertedFastq> fastq = conversion.samples().get(0).fastq();
        ConvertedFastq firstFastq = fastq.get(0);
        assertThat(firstFastq.sizeR1()).isEqualTo(SIZE_R1);
        assertThat(firstFastq.sizeR2()).hasValue(SIZE_R2);
        assertThat(firstFastq.md5R1()).isEqualTo(MD5_R1);
        assertThat(firstFastq.md5R2()).hasValue(MD5_R2);
    }

    @Test
    public void totalUndeterminedAndReadsCalculatedFromStats() {
        Conversion conversion = victim.apply(sampleSheet(), defaultStats());

        assertThat(conversion.undeterminedReads()).isEqualTo(1);
        assertThat(conversion.totalReads()).isEqualTo(4);
    }

    @Test
    public void sampleYieldAndQ30CalculatedFromFastq() {
        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second));
        Conversion conversion = victim.apply(sampleSheet(), defaultStats());

        assertThat(conversion.samples()).hasSize(1);
        ConvertedSample sample = conversion.samples().get(0);
        assertThat(sample.yield()).isEqualTo(3);
        assertThat(sample.yieldQ30()).isEqualTo(3);
    }

    @Test
    public void outputPathsSetAndIncludeFlowcellName() {
        when(bucket.list(path)).thenReturn(Lists.newArrayList(first, second));
        Conversion conversion = victim.apply(sampleSheet(), defaultStats());
        List<ConvertedFastq> fastq = conversion.samples().get(0).fastq();
        ConvertedFastq firstFastq = fastq.get(0);
        assertThat(firstFastq.outputPathR1()).isEqualTo("GIAB12878_flowcell_S1_L001_R1_001.fastq.gz");
        assertThat(firstFastq.outputPathR2()).hasValue("GIAB12878_flowcell_S1_L001_R2_001.fastq.gz");
    }

    private static ImmutableStats defaultStats() {
        return stats(laneStats(1, 1, sampleStats(BARCODE, 3, 1, 2)));
    }

    private static ImmutableStats stats(final ImmutableLaneStats... lanes) {
        return TestStats.stats(FLOWCELL, lanes);
    }

    private static IlluminaSample illuminaSample() {
        return IlluminaSample.builder().barcode(BARCODE).project(PROJECT).sample(SAMPLE).build();
    }

    private ImmutableSampleSheet sampleSheet() {
        return SampleSheet.builder().experimentName(EXPERIMENT).addSamples(illuminaSample()).build();
    }
}