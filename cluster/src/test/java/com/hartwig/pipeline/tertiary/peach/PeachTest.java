package com.hartwig.pipeline.tertiary.peach;

import static java.lang.String.format;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.PEACH;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PeachTest extends TertiaryStageTest<PeachOutput> {

    private static final String REFERENCE_PEACH_GENOTYPE_TSV = "reference.peach.haplotypes.best.tsv";

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).shallow(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).shallow(false).build())).isTrue();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.PEACH_CALLS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "reference.peach.events.tsv")),
                new AddDatatype(DataType.PEACH_CALLS_PER_GENE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "reference.peach.gene.events.tsv")),
                new AddDatatype(DataType.PEACH_ALL_HAPLOTYPES,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "reference.peach.haplotypes.all.tsv")),
                new AddDatatype(DataType.PEACH_GENOTYPE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, REFERENCE_PEACH_GENOTYPE_TSV)),
                new AddDatatype(DataType.PEACH_QC,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "reference.peach.qc.tsv")));
    }

    @Override
    protected void validateOutput(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/peach",
                ResultsDirectory.defaultDirectory().path(REFERENCE_PEACH_GENOTYPE_TSV)));
    }

    @Override
    protected void validatePersistedOutput(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/peach/" + REFERENCE_PEACH_GENOTYPE_TSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.PEACH_GENOTYPE, "peach/" + REFERENCE_PEACH_GENOTYPE_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "peach/" + REFERENCE_PEACH_GENOTYPE_TSV));
    }

    @Override
    protected Stage<PeachOutput, SomaticRunMetadata> createVictim() {
        return new Peach(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                toolCommand(PEACH)
                        + " -vcf_file /data/input/tumor.purple.germline.vcf.gz "
                        + "-sample_name reference "
                        + "-haplotypes_file /opt/resources/peach/37/haplotypes.37.tsv "
                        + "-function_file /opt/resources/peach/37/haplotype_functions.37.tsv "
                        + "-drugs_file /opt/resources/peach/37/peach_drugs.37.tsv "
                        + "-output_dir /data/output");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.germline.vcf.gz", "tumor.purple.germline.vcf.gz"));
    }
}