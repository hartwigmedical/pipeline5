package com.hartwig.pipeline.cram;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CramOutputTest {
    @Test
    public void shouldExtractFilenameForCrai() {
        assertThat(CramOutput.crai("/data/output/123r.cram")).isEqualTo("/data/output/123r.cram.crai");
    }
}