package com.hartwig.pipeline.tertiary.cuppa;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CuppaTest extends TertiaryStageTest<CuppaOutput> {

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).runCuppa(false).build())).isFalse();
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).runCuppa(false).build())).isFalse();
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).runCuppa(true).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).runCuppa(true).build())).isTrue();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.CUPPA_CHART,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), Cuppa.NAMESPACE, "tumor.cuppa.chart.png")));
    }

    @Override
    protected void setupPersistedDataset() {
    }

    @Override
    protected void validatePersistedOutput(final CuppaOutput output) {
        assertThat(output.chart()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/cuppa/tumor.cuppa.chart.png"));
    }

    @Override
    protected Stage<CuppaOutput, SomaticRunMetadata> createVictim() {
        return new Cuppa(purpleOutput(), linxOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx4G -jar /opt/tools/cuppa/1.4/cuppa.jar -categories DNA -ref_data_dir /opt/resources/cuppa "
                        + "-sample_data tumor -sample_data_dir /data/input -sample_sv_file /data/input/tumor.purple.sv.vcf.gz "
                        + "-sample_somatic_vcf /data/input/tumor.purple.somatic.vcf.gz -log_debug -output_dir /data/output",
                "/usr/bin/python3 /opt/tools/cuppa/1.4/cuppa_chart.py -sample tumor -sample_data /data/output/tumor "
                        + "-output_dir /data/output");
    }

    @Override
    protected void validateOutput(final CuppaOutput output) {
        // no additional validation
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.driver.catalog.tsv", "tumor.linx.driver.catalog.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.viral_inserts.tsv", "tumor.linx.viral_inserts.tsv"));
    }
}