package com.hartwig.batch.input;

import static com.hartwig.pipeline.testsupport.Resources.testResource;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class JsonInputParserTest {
    @Test
    public void shouldHandleMultipleObjectsInInputMembers() {
        String project = "hmf-project";
        JsonInputParser victim = new JsonInputParser();
        List<InputBundle> parsed = victim.parse(testResource("input-parsers/batch_descriptor.json"), project);
        assertThat(parsed.size()).isEqualTo(2);

        assertThat(parsed.get(0).get("input1").value()).isEqualTo("some-file");
        assertThat(parsed.get(0).get("input2").value()).isEqualTo("some-other-file");

        assertThat(parsed.get(1).get("input1").value()).isEqualTo("second-file");
        assertThat(parsed.get(1).get("input2").value()).isEqualTo("second-alternate");
    }
}