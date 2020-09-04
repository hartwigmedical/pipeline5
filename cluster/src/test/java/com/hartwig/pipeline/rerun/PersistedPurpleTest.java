package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedPurpleTest extends AbstractPersistedStageTest<PurpleOutput, SomaticRunMetadata> {
    @Override
    protected void assertOutput(final PurpleOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple", true));
        assertThat(output.somaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple/tumor.purple.somatic.vcf.gz"));
        assertThat(output.structuralVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple/tumor.purple.sv.vcf.gz"));
    }

    @Override
    protected SomaticRunMetadata metadata() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected PersistedStage<PurpleOutput, SomaticRunMetadata> createStage(final Stage<PurpleOutput, SomaticRunMetadata> decorated,
            final Arguments testDefaults, final String set) {
        return new PersistedPurple(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return Purple.NAMESPACE;
    }
}
