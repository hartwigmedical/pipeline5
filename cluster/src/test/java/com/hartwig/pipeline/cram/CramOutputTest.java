package com.hartwig.pipeline.cram;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CramOutputTest {
    @Test
    public void shouldExtractFilenameForCram() {
        assertThat(CramOutput.cramFile("/data/input/some-bam_123.bam")).isEqualTo("some-bam_123.cram");
    }

    @Test
    public void shouldExtractFilenameForCrai() {
        assertThat(CramOutput.craiFile("/data/input/some-bam_123.bam")).isEqualTo("some-bam_123.cram.crai");
    }
}