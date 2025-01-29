package com.hartwig.pipeline.alignment;

import static com.hartwig.pipeline.testsupport.TestInputs.BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.SET;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;

import org.junit.Test;

public class RunTest {

    private static final String REFERENCE_SAMPLE_NAME = "rsample";
    private static final String TUMOR_SAMPLE_NAME = "tsample";
    private static final String SOMATIC_STAGE_PREFIX = "som";
    private static final String REFERENCE_STAGE_PREFIX = "ref";
    private static final SingleSampleRunMetadata REFERENCE_SAMPLE = sample(SingleSampleRunMetadata.SampleType.REFERENCE, REFERENCE_SAMPLE_NAME);
    private static final SingleSampleRunMetadata TUMOR_SAMPLE = sample(SingleSampleRunMetadata.SampleType.TUMOR, TUMOR_SAMPLE_NAME);
    private static final SomaticRunMetadata SOMATIC = somatic(REFERENCE_SAMPLE, TUMOR_SAMPLE);

    private static SomaticRunMetadata somatic(final SingleSampleRunMetadata referenceSample, final SingleSampleRunMetadata tumorSample) {
        return SomaticRunMetadata.builder().maybeReference(referenceSample).maybeTumor(tumorSample).set("test").bucket(BUCKET).build();
    }

    private static SingleSampleRunMetadata sample(final SingleSampleRunMetadata.SampleType type, final String sampleId) {
        return SingleSampleRunMetadata.builder().type(type).barcode(sampleId).turquoiseSubject(sampleId).bucket(BUCKET).set(SET).build();
    }

    @Test
    public void idConsistsOfSampleNameAndUserWhenArgumentEmptySingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo(String.format("run-%s-%s-test", REFERENCE_STAGE_PREFIX, REFERENCE_SAMPLE_NAME));
    }

    @Test
    public void idCanBeOverriddenFromArgumentsSingleSample() {
        Run victim = Run.from(REFERENCE_SAMPLE,
                Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.DEVELOPMENT).runTag("override").build());
        assertThat(victim.id()).isEqualTo(String.format("run-%s-%s-override", REFERENCE_STAGE_PREFIX, REFERENCE_SAMPLE_NAME));
    }

    @Test
    public void idConsistsOfBothSamplesInPair() {
        Run victim = Run.from(SOMATIC, Arguments.testDefaultsBuilder().profile(Arguments.DefaultsProfile.PRODUCTION).build());
        assertThat(victim.id()).isEqualTo(String.format("run-%s-%s-%s", SOMATIC_STAGE_PREFIX, TUMOR_SAMPLE_NAME, "test"));
    }

    @Test
    public void replacesUnderscoresWithDashes() {
        Run victim = Run.from(somatic(sample(SingleSampleRunMetadata.SampleType.REFERENCE, "reference_suf"),
                sample(SingleSampleRunMetadata.SampleType.TUMOR, "tumor_suf")), Arguments.testDefaults());
        assertThat(victim.id()).isEqualTo(String.format("run-%s-%s-%s", SOMATIC_STAGE_PREFIX, "tumor-suf", "test"));
    }

    @Test
    public void truncatesSampleNamesToEnsureRunIdUnder40CharsInPair() {
        Run victim = Run.from(SOMATIC, Arguments.testDefaults());
        assertThat(victim.id().length()).isLessThanOrEqualTo(40);
    }
}