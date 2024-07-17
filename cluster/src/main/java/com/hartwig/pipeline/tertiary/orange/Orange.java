package com.hartwig.pipeline.tertiary.orange;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutputLocations;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.*;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;
import com.hartwig.pipeline.tools.VersionUtils;

import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.ORANGE;

@Namespace(Orange.NAMESPACE)
public class Orange implements Stage<OrangeOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "orange";
    public static final String NAMESPACE_NO_GERMLINE = "orange_no_germline";

    private static final String ORANGE_OUTPUT_JSON = ".orange.json";
    private static final String ORANGE_OUTPUT_PDF = ".orange.pdf";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/" + Purple.NAMESPACE;
    private static final String LOCAL_LINX_SOMATIC_DIR = VmDirectories.INPUT + "/" + LinxSomatic.NAMESPACE;
    private static final String LOCAL_LINX_GERMLINE_DIR = VmDirectories.INPUT + "/" + LinxGermline.NAMESPACE;

    private final ResourceFiles resourceFiles;
    private final InputDownload refMetrics;
    private final InputDownload tumMetrics;
    private final InputDownload refFlagstat;
    private final InputDownload tumFlagstat;
    private final InputDownload purpleOutputDir;
    private final InputDownload sageGermlineGeneCoverageTsv;
    private final InputDownload sageSomaticRefSampleBqrPlot;
    private final InputDownload sageSomaticTumorSampleBqrPlot;
    private final InputDownload lilacQc;
    private final InputDownload lilacResult;
    private final InputDownload linxSomaticOutputDir;
    private final InputDownload linxGermlineDataDir;
    private final InputDownload chordPredictionTxt;
    private final InputDownload cuppaSummaryPlot;
    private final InputDownload cuppaResultCsv;
    private final InputDownloadIfBlobExists cuppaFeaturePlot;
    private final InputDownload cuppaChartPlot;
    private final InputDownload peachGenotypeTsv;
    private final InputDownload sigsAllocationTsv;
    private final InputDownload annotatedVirusTsv;
    private final boolean includeGermline;

    public Orange(final BamMetricsOutput tumorMetrics, final BamMetricsOutput referenceMetrics, final FlagstatOutput tumorFlagstat,
            final FlagstatOutput referenceFlagstat, final SageOutput sageSomaticOutput, final SageOutput sageGermlineOutput,
            final PurpleOutput purpleOutput, final ChordOutput chordOutput, final LilacOutput lilacOutput,
            final LinxGermlineOutput linxGermlineOutput, final LinxSomaticOutput linxSomaticOutput, final CuppaOutput cuppaOutput,
            final VirusInterpreterOutput virusOutput, final PeachOutput peachOutput, final SigsOutput sigsOutput,
            final ResourceFiles resourceFiles, final boolean includeGermline) {

        this.resourceFiles = resourceFiles;
        this.refMetrics = new InputDownload(referenceMetrics.metricsOutputFile());
        this.tumMetrics = new InputDownload(tumorMetrics.metricsOutputFile());
        this.refFlagstat = new InputDownload(referenceFlagstat.flagstatOutputFile());
        this.tumFlagstat = new InputDownload(tumorFlagstat.flagstatOutputFile());
        this.includeGermline = includeGermline;
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purpleOutputDir = new InputDownload(purpleOutputLocations.outputDirectory(), LOCAL_PURPLE_DIR);
        this.sageGermlineGeneCoverageTsv = new InputDownload(sageGermlineOutput.germlineGeneCoverage());
        this.sageSomaticRefSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticRefSampleBqrPlot());
        this.sageSomaticTumorSampleBqrPlot = new InputDownload(sageSomaticOutput.somaticTumorSampleBqrPlot());
        LinxSomaticOutputLocations linxSomaticOutputLocations = linxSomaticOutput.linxOutputLocations();
        this.linxSomaticOutputDir = new InputDownload(linxSomaticOutputLocations.outputDirectory(), LOCAL_LINX_SOMATIC_DIR);
        this.linxGermlineDataDir = new InputDownload(linxGermlineOutput.linxOutputLocations().outputDirectory(), LOCAL_LINX_GERMLINE_DIR);
        this.chordPredictionTxt = new InputDownload(chordOutput.predictions());
        CuppaOutputLocations cuppaOutputLocations = cuppaOutput.cuppaOutputLocations();
        this.cuppaResultCsv = new InputDownload(cuppaOutputLocations.resultCsv());
        this.cuppaSummaryPlot = new InputDownload(cuppaOutputLocations.summaryChartPng());
        this.cuppaFeaturePlot = new InputDownloadIfBlobExists(cuppaOutputLocations.featurePlot());
        this.cuppaChartPlot = new InputDownload(cuppaOutputLocations.conclusionChart());
        this.peachGenotypeTsv = new InputDownload(peachOutput.genotypes());
        this.sigsAllocationTsv = new InputDownload(sigsOutput.allocationTsv());
        this.annotatedVirusTsv = new InputDownload(virusOutput.virusAnnotations());
        this.lilacQc = initialiseOptionalLocation(lilacOutput.qc());
        this.lilacResult = initialiseOptionalLocation(lilacOutput.result());
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
        return includeGermline ? NAMESPACE : NAMESPACE_NO_GERMLINE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return buildCommands(metadata);
    }

    private List<BashCommand> buildCommands(final SomaticRunMetadata metadata) {

        final String pipelineVersionFilePath = VmDirectories.INPUT + "/orange_pipeline.version.txt";
        final String pipelineVersion = VersionUtils.pipelineMajorMinorVersion();
        final List<String> primaryTumorDoids = metadata.tumor().primaryTumorDoids();
        String primaryTumorDoidsString = "\"" + String.join(";", primaryTumorDoids) + "\"";
        String linxPlotDir = linxSomaticOutputDir.getLocalTargetPath() + "/plot";
        ImmutableList.Builder<String> argumentListBuilder = ImmutableList.<String>builder()
                .add("-output_dir",
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
                        "-sample_data_dir",
                        VmDirectories.INPUT,
                        "-purple_dir",
                        purpleOutputDir.getLocalTargetPath(),
                        "-purple_plot_dir",
                        purpleOutputDir.getLocalTargetPath() + "/plot",
                        "-linx_germline_dir",
                        linxGermlineDataDir.getLocalTargetPath(),
                        "-linx_plot_dir",
                        linxPlotDir,
                        "-linx_dir",
                        linxSomaticOutputDir.getLocalTargetPath(),
                        "-lilac_dir",
                        VmDirectories.INPUT,
                        "-sage_dir",
                        VmDirectories.INPUT,
                        "-pipeline_version_file",
                        pipelineVersionFilePath,
                        "-cohort_mapping_tsv",
                        resourceFiles.orangeCohortMapping(),
                        "-cohort_percentiles_tsv",
                        resourceFiles.orangeCohortPercentiles(),
                        "-driver_gene_panel",
                        resourceFiles.driverGenePanel(),
                        "-known_fusion_file",
                        resourceFiles.knownFusionData(),
                        "-ensembl_data_dir",
                        resourceFiles.ensemblDataCache(),
                        "-add_disclaimer");
        if (!includeGermline) {
            argumentListBuilder.add("-convert_germline_to_somatic");
        }
        metadata.tumor()
                .samplingDate()
                .ifPresent(sd -> argumentListBuilder.add("-experiment_date", DateTimeFormatter.ofPattern("yyMMdd").format(sd)));
        JavaJarCommand orangeJarCommand = new JavaJarCommand(ORANGE, argumentListBuilder.build());

        return List.of(new MkDirCommand(linxPlotDir),
                () -> "echo '" + pipelineVersion + "' | tee " + pipelineVersionFilePath,
                orangeJarCommand);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(namespace().replace("_", "-"))
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
        if (includeGermline) {
            final String orangePdf = metadata.tumor().sampleName() + ORANGE_OUTPUT_PDF;
            final String orangeJson = metadata.tumor().sampleName() + ORANGE_OUTPUT_JSON;
            return List.of(new AddDatatype(DataType.ORANGE_OUTPUT_JSON,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), orangeJson)),
                    new AddDatatype(DataType.ORANGE_OUTPUT_PDF, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), orangePdf)));
        } else {
            return List.of();
        }
    }
}
