package com.hartwig.pipeline.rerun;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.somatic.SageCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedSageTest extends AbstractPersistedStageTest<SomaticCallerOutput, SomaticRunMetadata> {
    @Override
    protected void assertOutput(final SomaticCallerOutput output) {
        assertThat(output.finalSomaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/sage/tumor.sage.somatic.filtered.vcf.gz"));
    }

    @Override
    protected SomaticRunMetadata metadata() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected PersistedStage<SomaticCallerOutput, SomaticRunMetadata> createStage(
            final Stage<SomaticCallerOutput, SomaticRunMetadata> decorated, final Arguments testDefaults, final String set) {
        return new PersistedSage(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return SageCaller.NAMESPACE;
    }
}
