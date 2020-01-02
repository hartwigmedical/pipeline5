package com.hartwig.batch.input;

import static com.hartwig.pipeline.testsupport.Resources.testResource;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class FlatInputParserTest {
    @Test
    public void shouldReadInputsDiscardingBlankLinesAndDuplicates() {
        FlatInputParser parser = new FlatInputParser();
        List<InputBundle> parsed = parser.parse(testResource("input-parsers/batch_descriptor.txt"), "hmf-project");
        assertThat(parsed.size()).isEqualTo(2);
        assertThat(parsed.get(0).get().remoteFilename()).isEqualTo("gs://some-bucket/some-file");
        assertThat(parsed.get(1).get().remoteFilename()).isEqualTo("gs://some-bucket/some-other-file");
    }
}