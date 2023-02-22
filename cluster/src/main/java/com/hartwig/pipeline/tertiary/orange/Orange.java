package com.hartwig.pipeline.tertiary.orange;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.java.JavaJarFileExistsCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.input.InputDependencyProvider;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutputLocations;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxGermlineOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutputLocations;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;
import com.hartwig.pipeline.tools.Versions;

@Namespace(Orange.NAMESPACE)
public class Orange implements Stage<OrangeOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "orange";

    private static final String ORANGE_OUTPUT_JSON = ".orange.json";
    private static final String ORANGE_OUTPUT_PDF = ".orange.pdf";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/" + Purple.NAMESPACE;
    private static final String LOCAL_LINX_SOMATIC_DIR = VmDirectories.INPUT + "/" + LinxSomatic.NAMESPACE;
    private static final String LOCAL_LINX_GERMLINE_DIR = VmDirectories.INPUT + "/" + LinxGermline.NAMESPACE;

    private final ResourceFiles resourceFiles;

    private InputDownload refMetrics;
    private InputDownload tumMetrics;
    private InputDownload refFlagstat;
    private InputDownload tumFlagstat;
    private InputDownload purpleOutputDir;
    private InputDownload sageGermlineGeneCoverageTsv;
    private InputDownload sageSomaticRefSampleBqrPlot;
    private InputDownload sageSomaticTumorSampleBqrPlot;
    private InputDownload lilacQc;
    private InputDownload lilacResult;
    private InputDownload linxSomaticOutputDir;
    private InputDownload linxGermlineDataDir;
    private InputDownload chordPredictionTxt;
    private InputDownload cuppaSummaryPlot;
    private InputDownload cuppaResultCsv;
    private InputDownloadIfBlobExists cuppaFeaturePlot;
    private InputDownload cuppaChartPlot;
    private InputDownload peachGenotypeTsv;
    private InputDownload sigsAllocationTsv;
    private InputDownload annotatedVirusTsv;

    public Orange(final ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
    }

    public void registerInput(InputDependencyProvider inputDependencyProvider, boolean stageRunning) {
        var referenceMetrics = inputDependencyProvider.registerInput(BamMetricsOutput.class, "reference");
        var tumorMetrics = inputDependencyProvider.registerInput(BamMetricsOutput.class, "tumor");
        var tumorFlagstat = inputDependencyProvider.registerInput(FlagstatOutput.class, "tumor");
        var referenceFlagstat = inputDependencyProvider.registerInput(FlagstatOutput.class, "reference");
        var sageSomaticOutput = inputDependencyProvider.registerInput(SageOutput.class, "somatic");
        var sageGermlineOutput = inputDependencyProvider.registerInput(SageOutput.class, "germline");
        var purpleOutput = inputDependencyProvider.registerInput(PurpleOutput.class);
        var chordOutput = inputDependencyProvider.registerInput(ChordOutput.class);
        var lilacOutput = inputDependencyProvider.registerInput(LilacOutput.class);
        var linxGermlineOutput = inputDependencyProvider.registerInput(LinxGermlineOutput.class);
        var linxSomaticOutput = inputDependencyProvider.registerInput(LinxSomaticOutput.class);
        var cuppaOutput = inputDependencyProvider.registerInput(CuppaOutput.class);
        var virusOutput = inputDependencyProvider.registerInput(VirusInterpreterOutput.class);
        var peachOutput = inputDependencyProvider.registerInput(PeachOutput.class);
        var sigsOutput = inputDependencyProvider.registerInput(SigsOutput.class);

        if (stageRunning) {
            this.refMetrics = new InputDownload(referenceMetrics.metricsOutputFile());
            this.tumMetrics = new InputDownload(tumorMetrics.metricsOutputFile());
            this.refFlagstat = new InputDownload(referenceFlagstat.flagstatOutputFile());
            this.tumFlagstat = new InputDownload(tumorFlagstat.flagstatOutputFile());
            this.purpleOutputDir = new InputDownload(purpleOutput.outputLocations().outputDirectory(), LOCAL_PURPLE_DIR);
            this.sageGermlineGeneCoverageTsv = new InputDownload(sageGermlineOutput.germlineGeneCoverage());
            this.sageSomaticRefSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticRefSampleBqrPlot());
            this.sageSomaticTumorSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticTumorSampleBqrPlot());
            this.linxSomaticOutputDir = new InputDownload(linxSomaticOutput.linxOutputLocations().outputDirectory(), LOCAL_LINX_SOMATIC_DIR);
            this.linxGermlineDataDir = new InputDownload(linxGermlineOutput.linxOutputLocations().outputDirectory(), LOCAL_LINX_GERMLINE_DIR);
            this.chordPredictionTxt = new InputDownload(chordOutput.predictions());
            CuppaOutputLocations cuppaOutputLocations = cuppaOutput.cuppaOutputLocations();
            this.cuppaResultCsv = new InputDownload(cuppaOutputLocations.resultCsv());
            this.cuppaSummaryPlot = new InputDownload(cuppaOutputLocations.summaryChartPng());
            this.cuppaFeaturePlot = new InputDownloadIfBlobExists(cuppaOutputLocations.featurePlot());
            this.cuppaChartPlot = new InputDownload(cuppaOutputLocations.conclusionChart());
            this.peachGenotypeTsv = new InputDownload(peachOutput.genotypes());
            this.sigsAllocationTsv = initialiseOptionalLocation(sigsOutput.maybeAllocationTsv());
            this.annotatedVirusTsv = new InputDownload(virusOutput.virusAnnotations());
            this.lilacQc = initialiseOptionalLocation(lilacOutput.qc());
            this.lilacResult = initialiseOptionalLocation(lilacOutput.result());
        }
    }

    @Override
    public String outputClassTag() {
        return OutputClassUtil.getOutputClassTag(OrangeOutput.class);
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(new MkDirCommand(LOCAL_LINX_SOMATIC_DIR),
                new MkDirCommand(LOCAL_LINX_GERMLINE_DIR),
                new MkDirCommand(LOCAL_PURPLE_DIR),
                refMetrics,
                tumMetrics,
                refFlagstat,
                tumFlagstat,
                sageGermlineGeneCoverageTsv,
                sageSomaticRefSampleBqrPlot,
                sageSomaticTumorSampleBqrPlot,
                purpleOutputDir,
                lilacQc,
                lilacResult,
                linxSomaticOutputDir,
                linxGermlineDataDir,
                cuppaFeaturePlot,
                cuppaChartPlot,
                cuppaResultCsv,
                cuppaSummaryPlot,
                chordPredictionTxt,
                annotatedVirusTsv,
                peachGenotypeTsv,
                sigsAllocationTsv);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) { return buildCommands(metadata); }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) { return Stage.disabled(); }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) { return Stage.disabled(); }

    private List<BashCommand> buildCommands(final SomaticRunMetadata metadata) {

        final String pipelineVersionFilePath = VmDirectories.INPUT + "/orange_pipeline.version.txt";
        final String pipelineVersion = Versions.pipelineMajorMinorVersion();
        final List<String> primaryTumorDoids = metadata.tumor().primaryTumorDoids();
        String primaryTumorDoidsString = "\"" + String.join(";", primaryTumorDoids) + "\"";
        String linxPlotDir = linxSomaticOutputDir.getLocalTargetPath() + "/plot";
        var orangeJarCommand = new JavaJarCommand("orange",
                Versions.ORANGE,
                "orange.jar",
                "16G",
                List.of("-output_dir",
                        VmDirectories.OUTPUT,
                        "-ref_genome_version",
                        resourceFiles.version().numeric(),
                        "-tumor_sample_id",
                        metadata.tumor().sampleName(),
                        "-reference_sample_id",
                        metadata.reference().sampleName(),
                        "-doid_json",
                        resourceFiles.doidJson(),
                        "-primary_tumor_doids",
                        primaryTumorDoidsString,
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
                        "-purple_data_directory",
                        purpleOutputDir.getLocalTargetPath(),
                        "-purple_plot_directory",
                        purpleOutputDir.getLocalTargetPath() + "/plot",
                        "-lilac_qc_csv",
                        lilacQc.getLocalTargetPath(),
                        "-lilac_result_csv",
                        lilacResult.getLocalTargetPath(),
                        "-linx_germline_data_directory",
                        linxGermlineDataDir.getLocalTargetPath(),
                        "-linx_plot_directory",
                        linxPlotDir,
                        "-linx_somatic_data_directory",
                        linxSomaticOutputDir.getLocalTargetPath(),
                        "-cuppa_result_csv",
                        cuppaResultCsv.getLocalTargetPath(),
                        "-cuppa_summary_plot",
                        cuppaSummaryPlot.getLocalTargetPath(),
                        "-cuppa_chart_plot",
                        cuppaChartPlot.getLocalTargetPath(),
                        "-chord_prediction_txt",
                        chordPredictionTxt.getLocalTargetPath(),
                        "-peach_genotype_tsv",
                        peachGenotypeTsv.getLocalTargetPath(),
                        "-sigs_allocation_tsv",
                        sigsAllocationTsv.getLocalTargetPath(),
                        "-annotated_virus_tsv",
                        annotatedVirusTsv.getLocalTargetPath(),
                        "-pipeline_version_file",
                        pipelineVersionFilePath,
                        "-cohort_mapping_tsv",
                        resourceFiles.orangeCohortMapping(),
                        "-cohort_percentiles_tsv",
                        resourceFiles.orangeCohortPercentiles(),
                        "-driver_gene_panel_tsv",
                        resourceFiles.driverGenePanel(),
                        "-known_fusion_file",
                        resourceFiles.knownFusionData(),
                        "-ensembl_data_directory",
                        resourceFiles.ensemblDataCache(),
                        "-convert_germline_to_somatic"
                ));
        var withOptionalPlotCommand = new JavaJarFileExistsCommand(orangeJarCommand,
                "-cuppa_feature_plot", cuppaFeaturePlot.getLocalTargetPath());
        return List.of(new MkDirCommand(linxPlotDir),
                () -> "echo '" + pipelineVersion + "' | tee " + pipelineVersionFilePath, withOptionalPlotCommand);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 18))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public OrangeOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return OrangeOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
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

    @Override
    public OrangeOutput persistedOutput(final SomaticRunMetadata metadata) {
        return OrangeOutput.builder().status(PipelineStatus.PERSISTED).addAllDatatypes(addDatatypes(metadata)).build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        final String orangePdf = metadata.tumor().sampleName() + ORANGE_OUTPUT_PDF;
        final String orangeJson = metadata.tumor().sampleName() + ORANGE_OUTPUT_JSON;
        return List.of(new AddDatatype(DataType.ORANGE_OUTPUT_JSON,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), orangeJson)),
                new AddDatatype(DataType.ORANGE_OUTPUT_PDF, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), orangePdf)));
    }
}
