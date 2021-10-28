package com.hartwig.pipeline.tertiary.orange;

import java.util.List;
import java.util.function.Function;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutputLocations;
import com.hartwig.pipeline.tertiary.linx.Linx;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutputLocations;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.protect.ProtectOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.virus.VirusOutput;
import com.hartwig.pipeline.tools.Versions;

public class Orange implements Stage<OrangeOutput, SomaticRunMetadata> {
    static final String NAMESPACE = "orange";

    private static final String ORANGE_OUTPUT_JSON = ".orange.json";
    private static final String ORANGE_OUTPUT_PDF = ".orange.pdf";
    private static final String MAX_EVIDENCE_LEVEL = "C";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/" + Purple.NAMESPACE;
    private static final String LOCAL_LINX_DIR = VmDirectories.INPUT + "/" + Linx.NAMESPACE;

    private final ResourceFiles resourceFiles;
    private final InputDownload refMetrics;
    private final InputDownload tumMetrics;
    private final InputDownload refFlagstat;
    private final InputDownload tumFlagstat;
    private final InputDownload purpleGermlineVcf;
    private final InputDownload purpleSomaticVcf;
    private final InputDownload purplePurityTsv;
    private final InputDownload purpleQCFile;
    private final InputDownload purpleGeneCopyNumberTsv;
    private final InputDownload purpleSomaticDriverCatalog;
    private final InputDownload purpleGermlineDriverCatalog;
    private final InputDownload purpleOutputDir;
    private final InputDownload sageGermlineGeneCoverageTsv;
    private final InputDownload sageSomaticRefSampleBqrPlot;
    private final InputDownload sageSomaticTumorSampleBqrPlot;
    private final InputDownload linxOutputDir;
    private final InputDownload linxFusionTsv;
    private final InputDownload linxBreakEndTsv;
    private final InputDownload linxDriverCatalogTsv;
    private final InputDownload linxDriverTsv;
    private final InputDownload chordPredictionTxt;
    private final InputDownload cuppaConclusionTxt;
    private final InputDownload cuppaSummaryPlot;
    private final InputDownload cuppaResultCsv;
    private final InputDownload cuppaFeaturePlot;
    private final InputDownload peachGenotypeTsv;
    private final InputDownload protectEvidenceTsv;
    private final InputDownload annotatedVirusTsv;

    public Orange(final BamMetricsOutput tumorMetrics, final BamMetricsOutput referenceMetrics, final FlagstatOutput tumorFlagstat,
            final FlagstatOutput referenceFlagstat, final SageOutput sageSomaticOutput, final SageOutput sageGermlineOutput,
            final PurpleOutput purpleOutput, final ChordOutput chordOutput, final LinxOutput linxOutput, final CuppaOutput cuppaOutput,
            final VirusOutput virusOutput, final ProtectOutput protectOutput, final PeachOutput peachOutput,
            final ResourceFiles resourceFiles) {

        this.resourceFiles = resourceFiles;
        this.refMetrics = new InputDownload(referenceMetrics.metricsOutputFile());
        this.tumMetrics = new InputDownload(tumorMetrics.metricsOutputFile());
        this.refFlagstat = new InputDownload(referenceFlagstat.flagstatOutputFile());
        this.tumFlagstat = new InputDownload(tumorFlagstat.flagstatOutputFile());
        this.purpleGermlineVcf = new InputDownload(purpleOutput.outputLocations().germlineVcf());
        this.purpleSomaticVcf = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.purplePurityTsv = new InputDownload(purpleOutput.outputLocations().purityTsv());
        this.purpleQCFile = new InputDownload(purpleOutput.outputLocations().qcFile());
        this.purpleGeneCopyNumberTsv = new InputDownload(purpleOutput.outputLocations().geneCopyNumberTsv());
        this.purpleSomaticDriverCatalog = new InputDownload(purpleOutput.outputLocations().somaticDriverCatalog());
        this.purpleGermlineDriverCatalog = new InputDownload(purpleOutput.outputLocations().germlineDriverCatalog());
        this.purpleOutputDir = new InputDownload(purpleOutput.outputLocations().outputDirectory(), LOCAL_PURPLE_DIR);
        this.sageGermlineGeneCoverageTsv = new InputDownload(sageGermlineOutput.germlineGeneCoverageTsv());
        this.sageSomaticRefSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticRefSampleBqrPlot());
        this.sageSomaticTumorSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticTumorSampleBqrPlot());
        this.linxOutputDir = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::outputDirectory), LOCAL_LINX_DIR);
        this.linxFusionTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::fusions));
        this.linxBreakEndTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::breakends));
        this.linxDriverCatalogTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::driverCatalog));
        this.linxDriverTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::drivers));
        this.chordPredictionTxt = new InputDownload(chordOutput.maybePredictions().orElse(GoogleStorageLocation.empty()));
        this.cuppaConclusionTxt = new InputDownload(cuppaOrEmpty(cuppaOutput, CuppaOutputLocations::conclusionTxt));
        this.cuppaResultCsv = new InputDownload(cuppaOrEmpty(cuppaOutput, CuppaOutputLocations::resultCsv));
        this.cuppaSummaryPlot = new InputDownload(cuppaOrEmpty(cuppaOutput, CuppaOutputLocations::summaryChartPng));
        this.cuppaFeaturePlot = new InputDownload(cuppaOrEmpty(cuppaOutput, CuppaOutputLocations::featurePlot));
        this.peachGenotypeTsv = new InputDownload(peachOutput.maybeGenotypeTsv().orElse(GoogleStorageLocation.empty()));
        this.protectEvidenceTsv = new InputDownload(protectOutput.maybeEvidenceTsv().orElse(GoogleStorageLocation.empty()));
        this.annotatedVirusTsv = new InputDownload(virusOutput.maybeAnnotatedVirusFile().orElse(GoogleStorageLocation.empty()));
    }

    public GoogleStorageLocation linxOrEmpty(final LinxOutput linxOutput,
            final Function<LinxOutputLocations, GoogleStorageLocation> extractor) {
        return linxOutput.maybeLinxOutputLocations().map(extractor).orElse(GoogleStorageLocation.empty());
    }

    public GoogleStorageLocation cuppaOrEmpty(final CuppaOutput cuppaOutput,
            final Function<CuppaOutputLocations, GoogleStorageLocation> extractor) {
        return cuppaOutput.maybeCuppaOutputLocations().map(extractor).orElse(GoogleStorageLocation.empty());
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(new MkDirCommand(LOCAL_LINX_DIR),
                new MkDirCommand(LOCAL_PURPLE_DIR),
                purpleSomaticVcf,
                refMetrics,
                tumMetrics,
                refFlagstat,
                tumFlagstat,
                sageGermlineGeneCoverageTsv,
                sageSomaticRefSampleBqrPlot,
                sageSomaticTumorSampleBqrPlot,
                purpleOutputDir,
                purpleGermlineVcf,
                purplePurityTsv,
                purpleQCFile,
                purpleGeneCopyNumberTsv,
                purpleSomaticDriverCatalog,
                purpleGermlineDriverCatalog,
                linxOutputDir,
                linxFusionTsv,
                linxBreakEndTsv,
                linxDriverCatalogTsv,
                linxDriverTsv,
                cuppaConclusionTxt,
                cuppaResultCsv,
                cuppaSummaryPlot,
                cuppaFeaturePlot,
                chordPredictionTxt,
                protectEvidenceTsv,
                annotatedVirusTsv,
                peachGenotypeTsv);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        final String pipelineVersionFilePath = VmDirectories.INPUT + "/orange_pipeline.version.txt";
        final String pipelineVersion = Versions.pipelineMajorMinorVersion();
        final List<String> primaryTumorDoids = metadata.tumor().primaryTumorDoids();
        String linxPlotDir = linxOutputDir.getLocalTargetPath() + "/plot";
        return List.of(new MkDirCommand(linxPlotDir),
                () -> "echo '" + pipelineVersion + "' | tee " + pipelineVersionFilePath,
                new JavaJarCommand("orange",
                        Versions.ORANGE,
                        "orange.jar",
                        "8G",
                        List.of("-output_dir",
                                VmDirectories.OUTPUT,
                                "-doid_json",
                                resourceFiles.doidJson(),
                                "-primary_tumor_doids",
                                primaryTumorDoids.isEmpty() ? "\"\"" : "\"" + String.join(";", primaryTumorDoids) + "\"",
                                "-max_evidence_level",
                                MAX_EVIDENCE_LEVEL,
                                "-tumor_sample_id",
                                metadata.tumor().sampleName(),
                                "-reference_sample_id",
                                metadata.reference().sampleName(),
                                "-ref_sample_wgs_metrics_file",
                                refMetrics.getLocalTargetPath(),
                                "-tumor_sample_wgs_metrics_file",
                                tumMetrics.getLocalTargetPath(),
                                "-ref_sample_flagstat_file",
                                refFlagstat.getLocalTargetPath(),
                                "-tumor_sample_flagstat_file",
                                tumFlagstat.getLocalTargetPath(),
                                "-sage_germline_gene_coverage_tsv",
                                sageGermlineGeneCoverageTsv.getLocalTargetPath(),
                                "-sage_somatic_ref_sample_bqr_plot",
                                sageSomaticRefSampleBqrPlot.getLocalTargetPath(),
                                "-sage_somatic_tumor_sample_bqr_plot",
                                sageSomaticTumorSampleBqrPlot.getLocalTargetPath(),
                                "-purple_gene_copy_number_tsv",
                                purpleGeneCopyNumberTsv.getLocalTargetPath(),
                                "-purple_germline_driver_catalog_tsv",
                                purpleGermlineDriverCatalog.getLocalTargetPath(),
                                "-purple_germline_variant_vcf",
                                purpleGermlineVcf.getLocalTargetPath(),
                                "-purple_plot_directory",
                                purpleOutputDir.getLocalTargetPath() + "/plot",
                                "-purple_purity_tsv",
                                purplePurityTsv.getLocalTargetPath(),
                                "-purple_qc_file",
                                purpleQCFile.getLocalTargetPath(),
                                "-purple_somatic_driver_catalog_tsv",
                                purpleSomaticDriverCatalog.getLocalTargetPath(),
                                "-purple_somatic_variant_vcf",
                                purpleSomaticVcf.getLocalTargetPath(),
                                "-linx_fusion_tsv",
                                linxFusionTsv.getLocalTargetPath(),
                                "-linx_breakend_tsv",
                                linxBreakEndTsv.getLocalTargetPath(),
                                "-linx_driver_catalog_tsv",
                                linxDriverCatalogTsv.getLocalTargetPath(),
                                "-linx_driver_tsv",
                                linxDriverTsv.getLocalTargetPath(),
                                "-linx_plot_directory",
                                linxPlotDir,
                                "-cuppa_conclusion_txt",
                                cuppaConclusionTxt.getLocalTargetPath(),
                                "-cuppa_result_csv",
                                cuppaResultCsv.getLocalTargetPath(),
                                "-cuppa_summary_plot",
                                cuppaSummaryPlot.getLocalTargetPath(),
                                "-cuppa_feature_plot",
                                cuppaFeaturePlot.getLocalTargetPath(),
                                "-chord_prediction_txt",
                                chordPredictionTxt.getLocalTargetPath(),
                                "-peach_genotype_tsv",
                                peachGenotypeTsv.getLocalTargetPath(),
                                "-protect_evidence_tsv",
                                protectEvidenceTsv.getLocalTargetPath(),
                                "-annotated_virus_tsv",
                                annotatedVirusTsv.getLocalTargetPath(),
                                "-pipeline_version_file",
                                pipelineVersionFilePath)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(2, 4))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public OrangeOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        final String orangePdf = metadata.tumor().sampleName() + ORANGE_OUTPUT_PDF;
        final String orangeJson = metadata.tumor().sampleName() + ORANGE_OUTPUT_JSON;
        return OrangeOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.ORANGE_OUTPUT_JSON,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), orangeJson)),
                        new AddDatatype(DataType.ORANGE_OUTPUT_PDF,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), orangePdf)))
                .build();
    }

    @Override
    public OrangeOutput skippedOutput(final SomaticRunMetadata metadata) {
        return OrangeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
