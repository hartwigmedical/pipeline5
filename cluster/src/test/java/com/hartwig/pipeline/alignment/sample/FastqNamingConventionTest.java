package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class FastqNamingConventionTest {

    @Test
    public void trueWhenConventionIsMatched() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_S11_L001_R1_001.fastq.gz")).isTrue();
    }

    @Test
    public void missingFlowcell() {
        assertThat(test("CPCT12345678R_S1_L001_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void missingSample() {
        assertThat(test("HJJLGCCXX_S1_L001_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void missingSampleIndex() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_L001_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void missingLaneIndex() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_S1_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void missingPairId() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_S1_L001_001.fastq.gz")).isFalse();
    }

    @Test
    public void nonNumericSampleIndex() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_SA_L001_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void nonNumericLaneIndex() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_SA_LABC_R1_001.fastq.gz")).isFalse();
    }

    @Test
    public void impossiblePairEnd() {
        assertThat(test("CPCT12345678R_HJJLGCCXX_SA_L001_R3_001.fastq.gz")).isFalse();
    }

    private static boolean test(final String s) {
        return new FastqNamingConvention().test(s);
    }
}