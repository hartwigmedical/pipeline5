package com.hartwig.pipeline.tertiary.rose;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class RoseTest extends TertiaryStageTest<RoseOutput> {

    @Override
    protected Stage<RoseOutput, SomaticRunMetadata> createVictim() {
        return new Rose(TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.purpleOutput(),
                TestInputs.linxSomaticOutput(),
                TestInputs.virusInterpreterOutput(),
                TestInputs.chordOutput(),
                TestInputs.cuppaOutput());
    }

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).shallow(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).shallow(false).build())).isTrue();
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx8G -jar /opt/tools/rose/1.3.1/rose.jar" + " -actionability_database_tsv /opt/resources/rose/actionability.tsv"
                        + " -ref_genome_version 37" + " -driver_gene_tsv /opt/resources/gene_panel/37/DriverGenePanel.37.tsv"
                        + " -purple_purity_tsv /data/input/tumor.purple.purity.tsv" + " -purple_qc_file /data/input/tumor.purple.qc"
                        + " -purple_gene_copy_number_tsv /data/input/tumor.purple.cnv.gene.tsv"
                        + " -purple_somatic_driver_catalog_tsv /data/input/tumor.driver.catalog.somatic.tsv"
                        + " -purple_germline_driver_catalog_tsv /data/input/tumor.driver.catalog.germline.tsv"
                        + " -purple_somatic_variant_vcf /data/input/tumor.purple.somatic.vcf.gz"
                        + " -purple_germline_variant_vcf /data/input/tumor.purple.germline.vcf.gz"
                        + " -linx_fusion_tsv /data/input/tumor.linx.fusion.tsv" + " -linx_breakend_tsv /data/input/tumor.linx.breakend.tsv"
                        + " -linx_driver_catalog_tsv /data/input/tumor.linx.driver.catalog.tsv"
                        + " -annotated_virus_tsv /data/input/tumor.virus.annotated.tsv"
                        + " -chord_prediction_txt /data/input/tumor_chord_prediction.txt"
                        + " -cuppa_result_csv /data/input/tumor.cup.data.csv -output_dir /data/output"
                        + " -tumor_sample_id tumor" + " -ref_sample_id reference"
                        + " -patient_id not_used_because_primary_tumor_tsv_has_only_headers");
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.somatic.tsv", "tumor.driver.catalog.somatic.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.germline.tsv", "tumor.driver.catalog.germline.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.germline.vcf.gz", "tumor.purple.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.driver.catalog.tsv", "tumor.linx.driver.catalog.tsv"),
                input(expectedRuntimeBucketName() + "/virusintrprtr/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cup.data.csv", "tumor.cup.data.csv"));
    }

    @Override
    protected void validateOutput(final RoseOutput output) {
        // Output is not used elsewhere yet
    }

    @Override
    protected void validatePersistedOutput(final RoseOutput output) {
        // Persisted output not used elsewhere
    }
}