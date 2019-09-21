package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.junit.Test;

public class RunTest {

    private static final SingleSampleRunMetadata REFERENCE_SAMPLE = SingleSampleRunMetadata.builder().sampleId("reference").build();
    private static final SingleSampleRunMetadata TUMOR_SAMPLE = SingleSampleRunMetadata.builder().sampleId("tumor").build();
    private static final SomaticRunMetadata SOMATIC =
            SomaticRunMetadata.builder().reference(REFERENCE_SAMPLE).maybeTumor(TUMOR_SAMPLE).runName("test").build();

    @Test
    public void idConsistsOfSampleNameAndUserWhenArgumentEmptySingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo("run-reference-test");
    }

    @Test
    public void idCanBeOverriddenFromArgumentsSingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).runId("override").build());
        assertThat(victim.id()).isEqualTo("run-reference-override");
    }

    @Test
    public void idConsistsOfBothSamplesInPair() {
        Run victim = Run.from(SOMATIC, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo("run-reference-tumor-test");
    }

    @Test
    public void replacesUnderscoresWithDashes() {
        Run victim = Run.from(SOMATIC, Arguments.testDefaults());
        assertThat(victim.id()).isEqualTo("run-reference-suf-tumor-suf-test");
    }

    @Test
    public void truncatesSampleNamesToEnsureRunIdUnder40CharsInPair() {
        Run victim = Run.from(SOMATIC, Arguments.testDefaults());
        assertThat(victim.id().length()).isLessThanOrEqualTo(40);
    }

    @Test
    public void appendsSbpRunIdWhenSpecified() {
        Run victim = Run.from(REFERENCE_SAMPLE,
                Arguments.testDefaultsBuilder()
                        .profile(Arguments.DefaultsProfile.PRODUCTION)
                        .sbpApiRunId(1)
                        .runId(Optional.empty())
                        .build());
        assertThat(victim.id()).isEqualTo("run-reference-1");
    }
}