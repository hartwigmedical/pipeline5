package com.hartwig.pipeline.tertiary.orange;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class OrangeTest extends TertiaryStageTest<OrangeOutput> {

    @Override
    protected Stage<OrangeOutput, SomaticRunMetadata> createVictim() {
        return new Orange(TestInputs.tumorMetricsOutput(),
                TestInputs.referenceMetricsOutput(),
                TestInputs.tumorFlagstatOutput(),
                TestInputs.referenceFlagstatOutput(),
                TestInputs.sageSomaticOutput(),
                TestInputs.sageGermlineOutput(),
                TestInputs.purpleOutput(),
                TestInputs.chordOutput(),
                TestInputs.linxOutput(),
                TestInputs.cuppaOutput(),
                TestInputs.virusOutput(),
                TestInputs.protectOutput(),
                TestInputs.peachOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.somatic.tsv", "tumor.driver.catalog.somatic.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.germline.tsv", "tumor.driver.catalog.germline.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.germline.vcf.gz", "tumor.purple.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.driver.catalog.tsv", "tumor.linx.driver.catalog.tsv"),
                input(expectedRuntimeBucketName() + "/virusbreakend/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"),
                input("run-reference-test/bam_metrics/results/reference.wgsmetrics", "reference.wgsmetrics"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "tumor.wgsmetrics"),
                input("run-reference-test/flagstat/reference.flagstat", "reference.flagstat"),
                input("run-tumor-test/flagstat/tumor.flagstat", "tumor.flagstat"),
                input(expectedRuntimeBucketName() + "/sage_germline/results/tumorsage.gene.coverage.tsv", "tumorsage.gene.coverage.tsv"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/referencesage.bqr.png", "referencesage.bqr.png"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/tumorsage.bqr.png", "tumorsage.bqr.png"),
                input(expectedRuntimeBucketName() + "/purple/results/", "purple"),
                input(expectedRuntimeBucketName() + "/linx/results/", "linx"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.drivers.tsv", "tumor.linx.drivers.tsv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.conclusion.txt", "tumor.cuppa.conclusion.txt"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cup.data.csv", "tumor.cup.data.csv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.chart.png", "tumor.cuppa.chart.png"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cup.report.summary.png", "tumor.cup.report.summary.png"),
                input(expectedRuntimeBucketName() + "/protect/tumor.protect.tsv", "tumor.protect.tsv"),
                input(expectedRuntimeBucketName() + "/peach/tumor.peach.genotype.tsv", "tumor.peach.genotype.tsv"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Arrays.asList("mkdir -p /data/input/linx/plot",
                "mkdir -p /data/input/purple/plot",
                "echo '5.24' | tee /data/input/orange_pipeline.version.txt",
                "java -Xmx8G -jar /opt/tools/orange/1.1/orange.jar -output_dir /data/output -doid_json /opt/resources/disease_ontology/201015_doid.json "
                        + "-primary_tumor_doids \"01;02\" -max_evidence_level C -tumor_sample_id tumor -reference_sample_id reference "
                        + "-ref_sample_wgs_metrics_file /data/input/reference.wgsmetrics -tumor_sample_wgs_metrics_file /data/input/tumor.wgsmetrics "
                        + "-ref_sample_flagstat_file /data/input/reference.flagstat -tumor_sample_flagstat_file /data/input/tumor.flagstat "
                        + "-sage_germline_gene_coverage_tsv /data/input/tumorsage.gene.coverage.tsv -sage_somatic_ref_sample_bqr_plot /data/input/referencesage.bqr.png "
                        + "-sage_somatic_tumor_sample_bqr_plot /data/input/tumorsage.bqr.png -purple_gene_copy_number_tsv /data/input/tumor.purple.cnv.gene.tsv "
                        + "-purple_germline_driver_catalog_tsv /data/input/tumor.driver.catalog.germline.tsv -purple_germline_variant_vcf /data/input/tumor.purple.germline.vcf.gz "
                        + "-purple_plot_directory /data/input/purple/plot -purple_purity_tsv /data/input/tumor.purple.purity.tsv -purple_qc_file /data/input/tumor.purple.qc "
                        + "-purple_somatic_driver_catalog_tsv /data/input/tumor.driver.catalog.somatic.tsv -purple_somatic_variant_vcf /data/input/tumor.purple.somatic.vcf.gz "
                        + "-linx_fusion_tsv /data/input/tumor.linx.fusion.tsv -linx_breakend_tsv /data/input/tumor.linx.breakend.tsv "
                        + "-linx_driver_catalog_tsv /data/input/tumor.linx.driver.catalog.tsv -linx_driver_tsv /data/input/tumor.linx.drivers.tsv "
                        + "-linx_plot_directory /data/input/linx/plot -cuppa_conclusion_txt /data/input/tumor.cuppa.conclusion.txt "
                        + "-cuppa_result_csv /data/input/tumor.cup.data.csv -cuppa_summary_plot /data/input/tumor.cup.report.summary.png "
                        + "-cuppa_feature_plot /data/input/tumor.cuppa.chart.png -chord_prediction_txt /data/input/tumor_chord_prediction.txt "
                        + "-peach_genotype_tsv /data/input/tumor.peach.genotype.tsv -protect_evidence_tsv /data/input/tumor.protect.tsv "
                        + "-annotated_virus_tsv /data/input/tumor.virus.annotated.tsv -pipeline_version_file /data/input/orange_pipeline.version.txt");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final OrangeOutput output) {
        // no further testing because Orange output does not serve as input for other tools
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.ORANGE_OUTPUT_JSON,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Orange.NAMESPACE, "tumor.orange.json")),
                new AddDatatype(DataType.ORANGE_OUTPUT_PDF,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Orange.NAMESPACE, "tumor.orange.pdf")));
    }
}