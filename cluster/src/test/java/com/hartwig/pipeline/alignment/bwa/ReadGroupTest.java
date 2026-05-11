package com.hartwig.pipeline.alignment.bwa;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ReadGroupTest {

    private static final String SAMPLE = "COLO829v003R";
    private static final String FLOWCELL = "HHKYHDSXX";

    @Test
    public void removesR1FromFastqFastqName() {
        assertThat(ReadGroup.fromFastq(SAMPLE, FLOWCELL, false, "COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz")).isEqualTo(
                expected("AHHKYHDSXX_S13_L001_001"));
    }

    @Test
    public void removesR2FromFastqFastqName() {
        assertThat(ReadGroup.fromFastq(SAMPLE, FLOWCELL, false, "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz")).isEqualTo(
                expected("AHHKYHDSXX_S13_L001_001"));
    }

    @Test
    public void removesFastqExtension() {
        assertThat(ReadGroup.fromFastq(SAMPLE, FLOWCELL, false, "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq")).isEqualTo(
                expected("AHHKYHDSXX_S13_L001_001"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingMissingFields() {
        ReadGroup.fromFastq(SAMPLE, FLOWCELL, true, "COLO829v003R_S13_L001_R2_001.fastq");
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingLaneNotCorrectFormat() {
        ReadGroup.fromFastq(SAMPLE, FLOWCELL, true, "COLO829v003R_AHHKYHDSXX_S13_L0A1_R2_001.fastq");
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingPositionInPairIncorrectFormat() {
        ReadGroup.fromFastq(SAMPLE, FLOWCELL, true, "COLO829v003R_AHHKYHDSXX_S13_L0A1_P2_001.fastq");
    }

    @Test
    public void strictNamingPasses() {
        assertThat(ReadGroup.fromFastq(SAMPLE, FLOWCELL, true, "COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz")).isEqualTo(
                expected("AHHKYHDSXX_S13_L001_001"));
    }

    private static String expected(final String id) {
        return String.format("\"@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:%s\\tSM:%s\"", id, SAMPLE, FLOWCELL, SAMPLE);
    }
}
