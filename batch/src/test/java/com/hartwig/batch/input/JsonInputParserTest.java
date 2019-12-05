package com.hartwig.batch.input;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class JsonInputParserTest {
    @Test
    public void shouldHandleMultipleObjectsInInputMembers() {
        String project = "hmf-project";
        String inputFilePath = com.hartwig.pipeline.testsupport.Resources.testResource("input-parsers/batch_descriptor.json");
        JsonInputParser victim = new JsonInputParser(inputFilePath, project);

        List<List<InputFileDescriptor>> parsed = victim.parse();

        assertThat(parsed.size()).isEqualTo(2);

        assertThat(parsed.get(0).size()).isEqualTo(2);
        assertThat(parsed.get(0).get(0).name()).isEqualTo("input1");
        assertThat(parsed.get(0).get(0).remoteFilename()).isEqualTo("some-file");
        assertThat(parsed.get(0).get(1).name()).isEqualTo("input2");
        assertThat(parsed.get(0).get(1).remoteFilename()).isEqualTo("some-other-file");

        assertThat(parsed.get(1).get(0).name()).isEqualTo("input1");
        assertThat(parsed.get(1).get(0).remoteFilename()).isEqualTo("second-file");
        assertThat(parsed.get(1).get(1).name()).isEqualTo("input2");
        assertThat(parsed.get(1).get(1).remoteFilename()).isEqualTo("second-alternate");
    }
}