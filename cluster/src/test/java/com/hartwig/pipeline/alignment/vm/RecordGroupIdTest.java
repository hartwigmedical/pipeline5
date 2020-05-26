package com.hartwig.pipeline.alignment.vm;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class RecordGroupIdTest {

    @Test
    public void removesR1FromFastqName() {
        assertThat(RecordGroupId.from("COLO829v003R_AHHKYHDSXX_S13_L001_R1_001.fastq.gz")).isEqualTo("COLO829v003R_AHHKYHDSXX_S13_L001_001");
    }

    @Test
    public void removesR2FromFastqName() {
        assertThat(RecordGroupId.from("COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq.gz")).isEqualTo("COLO829v003R_AHHKYHDSXX_S13_L001_001");
    }

    @Test
    public void removesFastqExtension() {
        assertThat(RecordGroupId.from("COLO829v003R_AHHKYHDSXX_S13_L001_R2_001.fastq")).isEqualTo("COLO829v003R_AHHKYHDSXX_S13_L001_001");
    }
}