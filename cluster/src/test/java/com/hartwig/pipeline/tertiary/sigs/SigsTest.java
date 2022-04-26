package com.hartwig.pipeline.tertiary.sigs;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class SigsTest extends TertiaryStageTest<SigsOutput> {

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).build())).isTrue();
    }

    @Override
    protected Stage<SigsOutput, SomaticRunMetadata> createVictim() {
        return new Sigs(purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx4G -jar /opt/tools/sigs/1.0/sigs.jar -sample tumor "
                + "-signatures_file /opt/resources/sigs/snv_cosmic_signatures.csv -somatic_vcf_file /data/input/tumor.purple.somatic.vcf.gz "
                + "-output_dir /data/output");
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.SIGNATURE_ALLOCATION,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), Sigs.NAMESPACE, "tumor.sig.allocation.tsv")));
    }

    @Override
    protected void validateOutput(final SigsOutput output) {
        // Sigs output is not used as input for other tools
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final SigsOutput output) {
        // Sigs defines no persisted output
    }

    @Override
    protected void validatePersistedOutput(final SigsOutput output) {
        // Sigs defines no persisted output
    }
}