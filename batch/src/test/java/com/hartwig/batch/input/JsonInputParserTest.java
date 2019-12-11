package com.hartwig.batch.input;

import org.junit.Test;

import java.util.List;

import static com.hartwig.pipeline.testsupport.Resources.testResource;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class JsonInputParserTest {
    @Test
    public void shouldHandleMultipleObjectsInInputMembers() {
        String project = "hmf-project";
        JsonInputParser victim = new JsonInputParser(testResource("input-parsers/batch_descriptor.json"), project);
        List<InputBundle> parsed = victim.parse();
        assertThat(parsed.size()).isEqualTo(2);

        assertThat(parsed.get(0).get("input1").remoteFilename()).isEqualTo("some-file");
        assertThat(parsed.get(0).get("input2").remoteFilename()).isEqualTo("some-other-file");

        assertThat(parsed.get(1).get("input1").remoteFilename()).isEqualTo("second-file");
        assertThat(parsed.get(1).get("input2").remoteFilename()).isEqualTo("second-alternate");
    }
}