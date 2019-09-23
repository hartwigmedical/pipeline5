package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.ImmutableSingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class RunTest {

    private static final SingleSampleRunMetadata REFERENCE_SAMPLE = sample(SingleSampleRunMetadata.SampleType.REFERENCE, "reference");
    private static final SingleSampleRunMetadata TUMOR_SAMPLE = sample(SingleSampleRunMetadata.SampleType.TUMOR, "tumor");
    private static final SomaticRunMetadata SOMATIC = somatic(REFERENCE_SAMPLE, TUMOR_SAMPLE);

    @NotNull
    private static SomaticRunMetadata somatic(final SingleSampleRunMetadata referenceSample, final SingleSampleRunMetadata tumorSample) {
        return SomaticRunMetadata.builder().reference(referenceSample).maybeTumor(tumorSample).runName("test").build();
    }

    private static ImmutableSingleSampleRunMetadata sample(final SingleSampleRunMetadata.SampleType type, final String sampleId) {
        return SingleSampleRunMetadata.builder().type(type).sampleId(sampleId).build();
    }

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
        Run victim = Run.from(somatic(sample(SingleSampleRunMetadata.SampleType.REFERENCE, "reference_suf"),
                sample(SingleSampleRunMetadata.SampleType.TUMOR, "tumor_suf")), Arguments.testDefaults());
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