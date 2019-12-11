package com.hartwig.batch.input;

import org.junit.Test;

import java.util.List;

import static com.hartwig.pipeline.testsupport.Resources.testResource;
import static org.assertj.core.api.Assertions.assertThat;

public class FlatInputParserTest {
    @Test
    public void shouldReadInputsDiscardingBlankLinesAndDuplicates() {
        FlatInputParser parser = new FlatInputParser(testResource("input-parsers/batch_descriptor.txt"), "hmf-project");
        List<InputBundle> parsed = parser.parse();
        assertThat(parsed.size()).isEqualTo(2);
        assertThat(parsed.get(0).get().remoteFilename()).isEqualTo("gs://some-bucket/some-file");
        assertThat(parsed.get(1).get().remoteFilename()).isEqualTo("gs://some-bucket/some-other-file");
    }
}