package com.hartwig.pipeline.tertiary.orange;

import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.ORANGE;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class OrangeTest extends TertiaryStageTest<OrangeOutput> {

    private static BashCommand orangeCommand(Orange victim) {
        return victim.tumorReferenceCommands(TestInputs.defaultSomaticRunMetadata()).get(2);
    }

    private Orange constructOrange(final Pipeline.Context context, final boolean includeGermline) {
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
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                context,
                includeGermline);
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
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.tsv", "tumor.lilac.tsv"),
                input(expectedRuntimeBucketName() + "/lilac/tumor.lilac.qc.tsv", "tumor.lilac.qc.tsv"),
                input(expectedRuntimeBucketName() + "/peach/tumor.peach.genotype.tsv", "tumor.peach.genotype.tsv"),
                input(expectedRuntimeBucketName() + "/sigs/tumor.sig.allocation.tsv", "tumor.sig.allocation.tsv"));
    }

    @Test
    public void shouldAddResearchDisclaimerWhenResearchContext() {
        Orange victim = constructOrange(Pipeline.Context.RESEARCH, true);
        assertThat(orangeCommand(victim).asBash()).contains("-add_disclaimer");
    }

    @Test
    public void shouldAddGermlineToSomaticConversionAndChangeNamespaceWhenNotIncludeGermline() {
        Orange victim = constructOrange(Pipeline.Context.RESEARCH, false);
        assertThat(victim.namespace()).isEqualTo(Orange.NAMESPACE_NO_GERMLINE);
        assertThat(orangeCommand(victim).asBash()).contains("-convert_germline_to_somatic");
    }

    @Test
    public void shouldReturnNoFurtherOperationsWhenGermlineNotIncluded() {
        Orange victim = constructOrange(Pipeline.Context.RESEARCH, false);
        assertThat(victim.addDatatypes(defaultSomaticRunMetadata())).isEqualTo(Collections.emptyList());
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
        return constructOrange(Pipeline.Context.DIAGNOSTIC, true);
    }

    @Override
    protected List<String> expectedCommands() {
        String jarRunCommand = toolCommand(ORANGE) + " -output_dir /data/output " + "-experiment_type WGS " + "-ref_genome_version 37 " + "-tumor_sample_id tumor "
                + "-reference_sample_id reference " + "-doid_json /opt/resources/disease_ontology/doid.json "
                + "-primary_tumor_doids \"01;02\" " + "-ref_sample_wgs_metrics_file /data/input/reference.wgsmetrics "
                + "-tumor_sample_wgs_metrics_file /data/input/tumor.wgsmetrics "
                + "-ref_sample_flagstat_file /data/input/reference.flagstat " + "-tumor_sample_flagstat_file /data/input/tumor.flagstat "
                + "-sample_data_dir /data/input " + "-purple_dir /data/input/purple " + "-purple_plot_dir /data/input/purple/plot "
                + "-linx_germline_dir /data/input/linx_germline " + "-linx_plot_dir /data/input/linx/plot " + "-linx_dir /data/input/linx "
                + "-lilac_dir /data/input " + "-sage_dir /data/input " + "-pipeline_version_file /data/input/orange_pipeline.version.txt "
                + "-cohort_mapping_tsv /opt/resources/orange/cohort_mapping.tsv "
                + "-cohort_percentiles_tsv /opt/resources/orange/cohort_percentiles.tsv "
                + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                + "-known_fusion_file /opt/resources/fusions/37/known_fusion_data.37.csv "
                + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ -sampling_date 230519";

        return Arrays.asList("echo '5.34' | tee /data/input/orange_pipeline.version.txt", jarRunCommand);
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