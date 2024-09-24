package com.hartwig.pipeline.tertiary.orange;

import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_FLAG_COUNT_TSV;
import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_SUMMARY_TSV;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.ORANGE;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class OrangeTest extends TertiaryStageTest<OrangeOutput> {

    private static BashCommand orangeTumorReferenceCommand(Orange victim) {
        return victim.tumorReferenceCommands(TestInputs.defaultSomaticRunMetadata()).get(2);
    }

    private static BashCommand orangeTumorOnlyCommand(Orange victim) {
        return victim.tumorOnlyCommands(TestInputs.defaultSomaticRunMetadata()).get(2);
    }

    private Orange constructOrange(final boolean includeGermline, final boolean isTargeted) {
        return new Orange(
                TestInputs.tumorMetricsOutput(),
                TestInputs.referenceMetricsOutput(),
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
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                includeGermline,
                isTargeted);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of("mkdir -p /data/input/linx",
                "mkdir -p /data/input/purple",
                "mkdir -p /data/input/linx_germline",
                input(expectedRuntimeBucketName() + "/purple/results/", "purple"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"),
                input("run-reference-test/bam_metrics/reference" + BAM_METRICS_SUMMARY_TSV, "reference" + BAM_METRICS_SUMMARY_TSV),
                input("run-tumor-test/bam_metrics/tumor" + BAM_METRICS_SUMMARY_TSV, "tumor" + BAM_METRICS_SUMMARY_TSV),
                input("run-reference-test/bam_metrics/reference" + BAM_METRICS_FLAG_COUNT_TSV, "reference" + BAM_METRICS_FLAG_COUNT_TSV),
                input("run-tumor-test/bam_metrics/tumor" + BAM_METRICS_FLAG_COUNT_TSV, "tumor" + BAM_METRICS_FLAG_COUNT_TSV),
                /*
                input("run-reference-test/bam_metrics/results/reference.wgsmetrics", "reference.wgsmetrics"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "tumor.wgsmetrics"),
                input("run-reference-test/flagstat/reference.flagstat", "reference.flagstat"),
                input("run-tumor-test/flagstat/tumor.flagstat", "tumor.flagstat"),
                 */
                input(expectedRuntimeBucketName() + "/sage_germline/results/tumorsage.gene.coverage.tsv", "tumorsage.gene.coverage.tsv"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/referencesage.bqr.png", "referencesage.bqr.png"),
                input(expectedRuntimeBucketName() + "/sage_somatic/results/tumorsage.bqr.png", "tumorsage.bqr.png"),
                input(expectedRuntimeBucketName() + "/linx_germline/results/", "linx_germline"),
                input(expectedRuntimeBucketName() + "/linx/results/", "linx"),
                input(expectedRuntimeBucketName() + "/virusintrprtr/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.vis_data.tsv", "tumor.cuppa.vis_data.tsv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.pred_summ.tsv", "tumor.cuppa.pred_summ.tsv"),
                input(expectedRuntimeBucketName() + "/cuppa/tumor.cuppa.vis.png", "tumor.cuppa.vis.png"),
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.tsv", "tumor.lilac.tsv"),
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.qc.tsv", "tumor.lilac.qc.tsv"),
                input(expectedRuntimeBucketName() + "/peach/reference.peach.haplotypes.best.tsv", "reference.peach.haplotypes.best.tsv"),
                input(expectedRuntimeBucketName() + "/sigs/tumor.sig.allocation.tsv", "tumor.sig.allocation.tsv"));
    }

    @Test
    public void shouldAddGermlineToSomaticConversionAndChangeNamespaceWhenNotIncludeGermline() {
        Orange victim = constructOrange(false, false);
        assertThat(victim.namespace()).isEqualTo(Orange.NAMESPACE_NO_GERMLINE);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("-convert_germline_to_somatic");

        victim = constructOrange(false, true);
        assertThat(victim.namespace()).isEqualTo(Orange.NAMESPACE_NO_GERMLINE);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("-convert_germline_to_somatic");
    }

    @Test
    public void shouldNotAddGermlineToSomaticConversionAndChangeNamespaceWhenNotIncludeGermline() {
        Orange victim = constructOrange(true, false);
        assertThat(victim.namespace()).isEqualTo(Orange.NAMESPACE);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).doesNotContain("-convert_germline_to_somatic");

        victim = constructOrange(true, true);
        assertThat(victim.namespace()).isEqualTo(Orange.NAMESPACE);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).doesNotContain("-convert_germline_to_somatic");
    }

    @Test
    public void shouldReturnNoFurtherOperationsWhenGermlineNotIncluded() {
        Orange victim = constructOrange(false, false);
        assertThat(victim.addDatatypes(defaultSomaticRunMetadata())).isEqualTo(Collections.emptyList());

        victim = constructOrange(false, true);
        assertThat(victim.addDatatypes(defaultSomaticRunMetadata())).isEqualTo(Collections.emptyList());
    }

    @Test
    public void shouldSetRunModusByIsTargetedForTumorReference() {
        Orange victim = constructOrange(false, false);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("WGS");

        victim = constructOrange(false, true);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("PANEL");

        victim = constructOrange(true, false);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("WGS");

        victim = constructOrange(true, true);
        assertThat(orangeTumorReferenceCommand(victim).asBash()).contains("PANEL");
    }

    @Test
    public void shouldSetRunModusByIsTargetedForTumorOnly() {
        Orange victim = constructOrange(true, false);
        assertThat(orangeTumorOnlyCommand(victim).asBash()).contains("WGS");

        victim = constructOrange(true, true);
        assertThat(orangeTumorOnlyCommand(victim).asBash()).contains("PANEL");
    }

    @Test
    public void shouldSkipOrangeNoGermlineForTumorOnly() {
        Orange victim = constructOrange(false, false);
        assertThat(victim.tumorOnlyCommands(TestInputs.defaultSomaticRunMetadata())).isEqualTo(Collections.emptyList());

        victim = constructOrange(false, true);
        assertThat(victim.tumorOnlyCommands(TestInputs.defaultSomaticRunMetadata())).isEqualTo(Collections.emptyList());
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

    @Override
    protected void validatePersistedOutput(final OrangeOutput output) {
        // no validation
    }

    @Override
    protected Stage<OrangeOutput, SomaticRunMetadata> createVictim() {
        return constructOrange(true, false);
    }

    @Override
    protected List<String> expectedCommands() {
        String jarRunCommand = toolCommand(ORANGE)
                + " -output_dir /data/output"
                + " -experiment_type WGS"
                + " -ref_genome_version 37"
                + " -doid_json /opt/resources/disease_ontology/doid.json"
                + " -sample_data_dir /data/input"
                + " -purple_dir /data/input/purple"
                + " -purple_plot_dir /data/input/purple/plot"
                + " -lilac_dir /data/input"
                + " -pipeline_version_file /data/input/orange_pipeline.version.txt"
                + " -cohort_mapping_tsv /opt/resources/orange/cohort_mapping.tsv"
                + " -cohort_percentiles_tsv /opt/resources/orange/cohort_percentiles.tsv"
                + " -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv"
                + " -known_fusion_file /opt/resources/sv/37/known_fusion_data.37.csv"
                + " -ensembl_data_dir /opt/resources/ensembl_data_cache/37/"
                + " -signatures_etiology_tsv /opt/resources/sigs/signatures_etiology.tsv"
                + " -add_disclaimer"
                + " -tumor_sample_id tumor"
                + " -primary_tumor_doids \"01;02\""
                + " -tumor_metrics_dir /data/input"
                + " -linx_plot_dir /data/input/linx/plot"
                + " -linx_dir /data/input/linx"
                + " -sage_dir /data/input"
                + " -sampling_date 230519"
                + " -reference_sample_id reference"
                + " -ref_metrics_dir /data/input"
                + " -linx_germline_dir /data/input/linx_germline";

        return Arrays.asList("mkdir -p /data/input/linx/plot", "echo '6.0' | tee /data/input/orange_pipeline.version.txt", jarRunCommand);
    }

    @Override
    protected List<String> expectedTumorOnlyCommands() {
        String jarRunCommand = toolCommand(ORANGE)
                + " -output_dir /data/output"
                + " -experiment_type WGS"
                + " -ref_genome_version 37"
                + " -doid_json /opt/resources/disease_ontology/doid.json"
                + " -sample_data_dir /data/input"
                + " -purple_dir /data/input/purple"
                + " -purple_plot_dir /data/input/purple/plot"
                + " -lilac_dir /data/input"
                + " -pipeline_version_file /data/input/orange_pipeline.version.txt"
                + " -cohort_mapping_tsv /opt/resources/orange/cohort_mapping.tsv"
                + " -cohort_percentiles_tsv /opt/resources/orange/cohort_percentiles.tsv"
                + " -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv"
                + " -known_fusion_file /opt/resources/sv/37/known_fusion_data.37.csv"
                + " -ensembl_data_dir /opt/resources/ensembl_data_cache/37/"
                + " -signatures_etiology_tsv /opt/resources/sigs/signatures_etiology.tsv"
                + " -add_disclaimer"
                + " -tumor_sample_id tumor"
                + " -primary_tumor_doids \"01;02\""
                + " -tumor_metrics_dir /data/input"
                + " -linx_plot_dir /data/input/linx/plot"
                + " -linx_dir /data/input/linx"
                + " -sage_dir /data/input"
                + " -sampling_date 230519";

        return Arrays.asList("mkdir -p /data/input/linx/plot", "echo '6.0' | tee /data/input/orange_pipeline.version.txt", jarRunCommand);
    }

    @Override
    protected void validateOutput(final OrangeOutput output) {
        // no further testing because Orange output does not serve as input for other tools
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }
}