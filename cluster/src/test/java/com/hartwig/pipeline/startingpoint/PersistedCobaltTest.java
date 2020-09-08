package com.hartwig.pipeline.startingpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedCobaltTest extends AbstractPersistedStageTest<CobaltOutput, SomaticRunMetadata> {
    @Override
    protected void assertOutput(final CobaltOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/cobalt", true));
    }

    @Override
    protected SomaticRunMetadata metadata() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected PersistedStage<CobaltOutput, SomaticRunMetadata> createStage(final Stage<CobaltOutput, SomaticRunMetadata> decorated,
            final Arguments testDefaults, final String set) {
        return new PersistedCobalt(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return Cobalt.NAMESPACE;
    }
}
