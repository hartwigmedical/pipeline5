package com.hartwig.bcl2fastq;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

public class ConversionTest {

    private static final String SUBMISSION = "submission";
    private static final String BARCODE_1 = "barcode1";
    private static final String BARCODE_2 = "barcode2";

    @Test
    public void emptyListOfPathsReturnsEmptyConversion() {
        Conversion victim = Conversion.from(Collections.emptyList());
        assertThat(victim.samples()).isEmpty();
    }

    @Test
    public void transformsPathsToConversionSamplesAndFastq() {
        Conversion victim = Conversion.from(ImmutableList.of(path(1, 1, BARCODE_1), path(2, 1, BARCODE_1)));
        assertThat(victim.samples()).hasSize(1);
        ConvertedSample sample = victim.samples().get(0);
        assertThat(sample.barcode()).isEqualTo(BARCODE_1);
        assertThat(sample.fastq()).hasSize(1);
        ConvertedFastq fastq = sample.fastq().get(0);
        assertThat(fastq.pathR1()).isEqualTo(
                "run-bcl-conversion/bcl2fastq/results/Data/Intensities/BaseCalls/submission/barcode1/sample_S1_L001_R1_001.fastq.gz");
        assertThat(fastq.pathR2()).isEqualTo(
                "run-bcl-conversion/bcl2fastq/results/Data/Intensities/BaseCalls/submission/barcode1/sample_S1_L001_R2_001.fastq.gz");
    }

    @Test
    public void groupsFastqIntoSamplesByBarcode() {
        Conversion victim = Conversion.from(ImmutableList.of(path(1, 1, BARCODE_1),
                path(2, 1, BARCODE_1),
                path(1, 1, BARCODE_2),
                path(2, 1, BARCODE_2)));
        assertThat(victim.samples()).hasSize(2);
    }

    @Test
    public void groupsFastqIntoLanes() {
        Conversion victim = Conversion.from(ImmutableList.of(path(1, 1, BARCODE_1),
                path(2, 1, BARCODE_1),
                path(1, 2, BARCODE_1),
                path(2, 2, BARCODE_1)));
        assertThat(victim.samples()).hasSize(1);
        assertThat(victim.samples().get(0).fastq()).hasSize(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentExceptionsWhenPairIncomplete() {
        Conversion.from(ImmutableList.of(path(1, 1, BARCODE_1)));
    }

    private String path(int numInPair, int lane, final String barcode) {
        return String.format("run-bcl-conversion/bcl2fastq/results/Data/Intensities/BaseCalls/%s/%s/sample_S1_L00%s_R%s_001.fastq.gz",
                SUBMISSION,
                barcode,
                lane,
                numInPair);
    }
}