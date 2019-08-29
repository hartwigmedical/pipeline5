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
    public void idCanBeOverriddenFromArgumentsSingleSample() {
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

    @Test
    public void replacesUnderscoresWithDashes() {
        Run victim = Run.from(REFERENCE_SAMPLE + "_suf", TUMOR_SAMPLE + "_suf", Arguments.testDefaults());
        assertThat(victim.id()).isEqualTo("run-reference-suf-tumor-suf");
    }

    @Test
    public void truncatesSampleNamesToEnsureRunIdUnder40CharsInPair() {
        Run victim = Run.from("very-long-reference-sample-name", "very-long-tumor-sample-name-NNNNN", Arguments.testDefaults());
        assertThat(victim.id().length()).isLessThanOrEqualTo(35);
    }

    @Test
    public void appendsShallowOnShallowSingleSampleRuns() {
        Run victim = Run.from(REFERENCE_SAMPLE, Arguments.testDefaultsBuilder().shallow(true).build());
        assertThat(victim.id()).isEqualTo("run-reference-shallow");
    }

    @Test
    public void appendsShallowOnShallowSomaticRuns() {
        Run victim = Run.from(REFERENCE_SAMPLE, TUMOR_SAMPLE, Arguments.testDefaultsBuilder().shallow(true).build());
        assertThat(victim.id()).isEqualTo("run-reference-tumor-shallow");
    }

    @Test
    public void appendsShallowOnShallowBeforeRunTag() {
        Run victim = Run.from(REFERENCE_SAMPLE, TUMOR_SAMPLE, Arguments.testDefaultsBuilder().shallow(true).runId("override").build());
        assertThat(victim.id()).isEqualTo("run-reference-tumor-override-shallow");
    }
}