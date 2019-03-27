package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.Test;

public class RunTest {

    private static final String SAMPLE = "sample";
    private static final LocalDateTime NOW = LocalDateTime.of(2019, 3, 26, 17, 35, 0);

    @Test
    public void idConsistsOfSampleNameWhenArgumentEmpty() {
        Run victim = Run.from(SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build(), NOW);
        assertThat(victim.id()).isEqualTo("run-sample");
    }

    @Test
    public void idCanBeOveriddenFromArguments() {
        Run victim = Run.from(SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).runId("override").build(),
                NOW);
        assertThat(victim.id()).isEqualTo("run-sample-override");
    }

    @Test
    public void idHasTimestampAppendedWhenInDevelopmentProfile() {
        Run victim = Run.from(SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).build(), NOW);
        assertThat(victim.id()).isEqualTo("run-sample-20190326173500");
    }
}