package com.hartwig.pipeline.tertiary.peach;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class PeachTest extends TertiaryStageTest<PeachOutput> {

    private static final String TUMOR_PEACH_GENOTYPE_TSV = "tumor.peach.genotype.tsv";

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
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, "tumor.peach.calls.tsv")),
                new AddDatatype(DataType.PEACH_GENOTYPE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Peach.NAMESPACE, TUMOR_PEACH_GENOTYPE_TSV)));
    }

    @Override
    protected void validateOutput(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/peach",
                ResultsDirectory.defaultDirectory().path(TUMOR_PEACH_GENOTYPE_TSV)));
    }

    @Override
    protected void validatePersistedOutput(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/peach/" + TUMOR_PEACH_GENOTYPE_TSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.PEACH_GENOTYPE, "peach/" + TUMOR_PEACH_GENOTYPE_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final PeachOutput output) {
        assertThat(output.genotypes()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "peach/" + TUMOR_PEACH_GENOTYPE_TSV));
    }

    @Override
    protected Stage<PeachOutput, SomaticRunMetadata> createVictim() {
        return new Peach(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("/opt/tools/peach/1.7_venv/bin/python /opt/tools/peach/1.7/src/main.py "
                + "--vcf /data/input/tumor.purple.germline.vcf.gz --sample_t_id tumor --sample_r_id reference --tool_version 1.7 "
                + "--outputdir /data/output --panel /opt/resources/peach/peach.json");
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