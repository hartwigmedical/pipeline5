package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;

import org.junit.Test;

public class RunTest {

    private static final String REFERENCE_SAMPLE = "reference";
    private static final String TUMOR_SAMPLE = "tumor";

    @Test
    public void idConsistsOfSampleNameWhenArgumentEmptySingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo("run-reference");
    }

    @Test
    public void idCanBeOveriddenFromArgumentsSingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).runId("override").build());
        assertThat(victim.id()).isEqualTo("run-reference-override");
    }

    @Test
    public void idConsistsOfBothSamplesInPair() {
        Run victim = Run.from(REFERENCE_SAMPLE,
                TUMOR_SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo("run-reference-tumor");
    }
}