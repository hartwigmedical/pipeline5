package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class RunTest {

    private static final String SAMPLE = "sample";

    @Test
    public void idConsistsOfSampleNameWhenArgumentEmpty() {
        Run victim = Run.from(SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo("run-sample");
    }

    @Test
    public void idCanBeOveriddenFromArguments() {
        Run victim = Run.from(SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).runId("override").build());
        assertThat(victim.id()).isEqualTo("run-sample-override");
    }
}