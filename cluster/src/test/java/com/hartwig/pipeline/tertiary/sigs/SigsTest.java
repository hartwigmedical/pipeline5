package com.hartwig.pipeline.tertiary.sigs;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.SIGS;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
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
        return new Sigs(purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of(toolCommand(SIGS) + " -sample tumor "
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
        String bucketName = expectedRuntimeBucketName() + "/" + Sigs.NAMESPACE;
        assertThat(output.allocationTsv().bucket()).isEqualTo(bucketName);
        assertThat(output.allocationTsv().path()).isEqualTo("results/tumor.sig.allocation.tsv");
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