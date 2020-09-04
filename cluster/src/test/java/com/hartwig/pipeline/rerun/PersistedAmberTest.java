package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedAmberTest extends AbstractPersistedStageTest<AmberOutput, SomaticRunMetadata> {

    @Override
    protected void assertOutput(final AmberOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/amber", true));
    }

    @Override
    protected SomaticRunMetadata metadata() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected PersistedStage<AmberOutput, SomaticRunMetadata> createStage(final Stage<AmberOutput, SomaticRunMetadata> decorated,
            final Arguments testDefaults, final String set) {
        return new PersistedAmber(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return Amber.NAMESPACE;
    }
}