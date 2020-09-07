package com.hartwig.pipeline.startingpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PersistedStructuralCallerTest extends AbstractPersistedStageTest<StructuralCallerOutput, SomaticRunMetadata> {
    @Override
    protected void assertOutput(final StructuralCallerOutput output) {
        assertThat(output.filteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/tumor.gridss.somatic.filtered.vcf.gz"));
        assertThat(output.filteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/tumor.gridss.somatic.filtered.vcf.gz.tbi"));
        assertThat(output.fullVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/tumor.gridss.somatic.vcf.gz"));
        assertThat(output.fullVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/tumor.gridss.somatic.vcf.gz.tbi"));
    }

    @Override
    protected SomaticRunMetadata metadata() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected PersistedStage<StructuralCallerOutput, SomaticRunMetadata> createStage(
            final Stage<StructuralCallerOutput, SomaticRunMetadata> decorated, final Arguments testDefaults, final String set) {
        return new PersistedStructuralCaller(decorated, testDefaults, set);
    }

    @Override
    protected String namespace() {
        return StructuralCaller.NAMESPACE;
    }
}
