package com.hartwig.pipeline.alignment.bwa;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class RecordGroupIdTest {

    @Test
    public void removesR1FromFastqName() {
        assertThat(RecordGroupId.from(false, "COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz")).isEqualTo("AHHKYHDSXX_S13_L001_001");
    }

    @Test
    public void removesR2FromFastqName() {
        assertThat(RecordGroupId.from(false, "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz")).isEqualTo("AHHKYHDSXX_S13_L001_001");
    }

    @Test
    public void removesFastqExtension() {
        assertThat(RecordGroupId.from(false, "COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq")).isEqualTo("AHHKYHDSXX_S13_L001_001");
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingMissingFields() {
        RecordGroupId.from(true, "COLO829v003R_S13_L001_R2_001.fastq");
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingLaneNotCorrectFormat() {
        RecordGroupId.from(true, "COLO829v003R_AHHKYHDSXX_S13_L0A1_R2_001.fastq");
    }

    @Test(expected = IllegalArgumentException.class)
    public void strictNamingPositionInPairIncorrectFormat() {
        RecordGroupId.from(true, "COLO829v003R_AHHKYHDSXX_S13_L0A1_P2_001.fastq");
    }

    @Test
    public void strictNamingPasses() {
        assertThat(RecordGroupId.from(true, "COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz")).isEqualTo("AHHKYHDSXX_S13_L001_001");
    }
}