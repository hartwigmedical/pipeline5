package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class RunTest {

    private static final String SAMPLE = "sample";

    @Test
    public void idConsistsOfSampleNameWhenArgumentEmpty() {
        Run victim = Run.from(SAMPLE, Arguments.defaults());
        assertThat(victim.id()).isEqualTo("run-sample");
    }

    @Test
    public void idCanBeOveriddenFromArguments() {
        Run victim = Run.from(SAMPLE, Arguments.defaultsBuilder().runId("override").build());
        assertThat(victim.id()).isEqualTo("run-sample-override");
    }
}