package com.hartwig.pipeline.tertiary.orange;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
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
                TestInputs.lilacOutput(),
                TestInputs.linxGermlineOutput(),
                TestInputs.linxSomaticOutput(),
                TestInputs.cuppaOutput(),
                TestInputs.virusInterpreterOutput(),
                TestInputs.peachOutput(),
                TestInputs.sigsOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of("mkdir -p /data/input/linx",
                "mkdir -p /data/input/purple",
                "mkdir -p /data/input/linx_germline",
                input(expectedRuntimeBucketName() + "/purple/results/", "purple"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"),
                input("run-reference-test/bam_metrics/results/reference.wgsmetrics", "reference.wgsmetrics"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "tumor.wgsmetrics"),
                input("run-reference-test/flagstat/reference.flagstat", "reference.flagstat"),
                input("run-tumor-test/flagstat/tumor.flagstat", "tumor.flagstat"),
                input(expectedRuntimeBucketName() + "/sage_germline/results/tumorsage.gene.coverage.tsv", "tumorsage.gene.coverage.tsv"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/referencesage.bqr.png", "referencesage.bqr.png"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/tumorsage.bqr.png", "tumorsage.bqr.png"),
                input(expectedRuntimeBucketName() + "/linx_germline/results/", "linx_germline"),
                input(expectedRuntimeBucketName() + "/linx/results/", "linx"),
                "gsutil  -q stat  gs://run-reference-tumor-test/cuppa/tumor.cup.report.features.png; if [ $? == 0 ]; then  gsutil -o "
                        + "'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n "
                        + "gs://run-reference-tumor-test/cuppa/tumor.cup.report.features.png /data/input/tumor.cup.report.features.png ; fi",
                input(expectedRuntimeBucketName() + "/virusintrprtr/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cup.data.csv", "tumor.cup.data.csv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cup.report.summary.png", "tumor.cup.report.summary.png"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.chart.png", "tumor.cuppa.chart.png"),
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.csv", "tumor.lilac.csv"),
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.qc.csv", "tumor.lilac.qc.csv"),
                input(expectedRuntimeBucketName() + "/peach/tumor.peach.genotype.tsv", "tumor.peach.genotype.tsv"),
                input(expectedRuntimeBucketName() + "/sigs/tumor.sig.allocation.tsv", "tumor.sig.allocation.tsv"));
    }

    @Override
    protected List<String> expectedCommands() {
        String jarRunCommand = "java -Xmx16G -jar /opt/tools/orange/2.4/orange.jar " + "-output_dir /data/output " + "-ref_genome_version 37 "
                + "-tumor_sample_id tumor " + "-reference_sample_id reference " + "-doid_json /opt/resources/disease_ontology/doid.json "
                + "-primary_tumor_doids \"01;02\" "
                + "-ref_sample_wgs_metrics_file /data/input/reference.wgsmetrics "
                + "-tumor_sample_wgs_metrics_file /data/input/tumor.wgsmetrics "
                + "-ref_sample_flagstat_file /data/input/reference.flagstat " + "-tumor_sample_flagstat_file /data/input/tumor.flagstat "
                + "-sage_germline_gene_coverage_tsv /data/input/tumorsage.gene.coverage.tsv "
                + "-sage_somatic_ref_sample_bqr_plot /data/input/referencesage.bqr.png "
                + "-sage_somatic_tumor_sample_bqr_plot /data/input/tumorsage.bqr.png "
                + "-purple_data_directory /data/input/purple "
                + "-purple_plot_directory /data/input/purple/plot "
                + "-lilac_qc_csv /data/input/tumor.lilac.qc.csv "
                + "-lilac_result_csv /data/input/tumor.lilac.csv "
                + "-linx_germline_data_directory /data/input/linx_germline "
                + "-linx_plot_directory /data/input/linx/plot "
                + "-linx_somatic_data_directory /data/input/linx "
                + "-cuppa_result_csv /data/input/tumor.cup.data.csv " + "-cuppa_summary_plot /data/input/tumor.cup.report.summary.png "
                + "-cuppa_chart_plot /data/input/tumor.cuppa.chart.png "
                + "-chord_prediction_txt /data/input/tumor_chord_prediction.txt "
                + "-peach_genotype_tsv /data/input/tumor.peach.genotype.tsv "
                + "-sigs_allocation_tsv /data/input/tumor.sig.allocation.tsv "
                + "-annotated_virus_tsv /data/input/tumor.virus.annotated.tsv "
                + "-pipeline_version_file /data/input/orange_pipeline.version.txt "
                + "-cohort_mapping_tsv /opt/resources/orange/cohort_mapping.tsv "
                + "-cohort_percentiles_tsv /opt/resources/orange/cohort_percentiles.tsv "
                + "-driver_gene_panel_tsv /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                + "-known_fusion_file /opt/resources/fusions/37/known_fusion_data.37.csv "
                + "-ensembl_data_directory /opt/resources/ensembl_data_cache/37/ "
                + "-convert_germline_to_somatic";
        String cuppaFile = " -cuppa_feature_plot /data/input/tumor.cup.report.features.png";
        String fileExistsCommand = "if [ -e /data/input/tumor.cup.report.features.png ]; then " + jarRunCommand + cuppaFile + " ; else " + jarRunCommand + " ; fi";
        return Arrays.asList("mkdir -p /data/input/linx/plot", "echo '5.33' | tee /data/input/orange_pipeline.version.txt", fileExistsCommand);
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
    protected void validatePersistedOutput(final OrangeOutput output) {
        // no validation
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