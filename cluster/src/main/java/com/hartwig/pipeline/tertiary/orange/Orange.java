package com.hartwig.pipeline.tertiary.orange;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.ORANGE;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.computeengine.execution.vm.command.unix.MkDirCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
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
import com.hartwig.pipeline.tools.VersionUtils;

import org.jetbrains.annotations.NotNull;

@Namespace(Orange.NAMESPACE)
public class Orange implements Stage<OrangeOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "orange";
    public static final String NAMESPACE_NO_GERMLINE = "orange_no_germline";

    private static final String ORANGE_OUTPUT_JSON = ".orange.json";
    private static final String ORANGE_OUTPUT_PDF = ".orange.pdf";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/" + Purple.NAMESPACE;
    private static final String LOCAL_LINX_SOMATIC_DIR = VmDirectories.INPUT + "/" + LinxSomatic.NAMESPACE;
    private static final String LOCAL_LINX_GERMLINE_DIR = VmDirectories.INPUT + "/" + LinxGermline.NAMESPACE;
    private static final String PIPELINE_VERSION_FILE_PATH = VmDirectories.INPUT + "/orange_pipeline.version.txt";

    private final ResourceFiles resourceFiles;
    private final InputDownloadCommand refMetrics;
    private final InputDownloadCommand tumMetrics;
    private final InputDownloadCommand refFlagstat;
    private final InputDownloadCommand tumFlagstat;
    private final InputDownloadCommand purpleOutputDir;
    private final InputDownloadCommand sageGermlineGeneCoverageTsv;
    private final InputDownloadCommand sageSomaticRefSampleBqrPlot;
    private final InputDownloadCommand sageSomaticTumorSampleBqrPlot;
    private final InputDownloadCommand lilacQc;
    private final InputDownloadCommand lilacResult;
    private final InputDownloadCommand linxSomaticOutputDir;
    private final InputDownloadCommand linxGermlineDataDir;
    private final InputDownloadCommand chordPredictionTxt;
    private final InputDownloadCommand cuppaVisPlot;
    private final InputDownloadCommand cuppaVisData;
    private final InputDownloadCommand cuppaPredSumm;
    private final InputDownloadCommand peachGenotypeTsv;
    private final InputDownloadCommand sigsAllocationTsv;
    private final InputDownloadCommand annotatedVirusTsv;
    private final boolean includeGermline;
    private final boolean isTargeted;

    public Orange(final BamMetricsOutput tumorMetrics, final BamMetricsOutput referenceMetrics,
            final SageOutput sageSomaticOutput, final SageOutput sageGermlineOutput,
            final PurpleOutput purpleOutput, final ChordOutput chordOutput, final LilacOutput lilacOutput,
            final LinxGermlineOutput linxGermlineOutput, final LinxSomaticOutput linxSomaticOutput, final CuppaOutput cuppaOutput,
            final VirusInterpreterOutput virusOutput, final PeachOutput peachOutput, final SigsOutput sigsOutput,
            final ResourceFiles resourceFiles, final boolean includeGermline, final boolean isTargeted) {

        this.resourceFiles = resourceFiles;
        this.refMetrics = new InputDownloadCommand(referenceMetrics.outputLocations().summary());
        this.tumMetrics = new InputDownloadCommand(tumorMetrics.outputLocations().summary());
        this.refFlagstat = new InputDownloadCommand(referenceMetrics.outputLocations().flagCounts());
        this.tumFlagstat = new InputDownloadCommand(tumorMetrics.outputLocations().flagCounts());
        this.includeGermline = includeGermline;
        this.isTargeted = isTargeted;
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purpleOutputDir = new InputDownloadCommand(purpleOutputLocations.outputDirectory(), LOCAL_PURPLE_DIR);
        this.sageGermlineGeneCoverageTsv = new InputDownloadCommand(sageGermlineOutput.germlineGeneCoverage());
        this.sageSomaticRefSampleBqrPlot = new InputDownloadCommand(sageSomaticOutput.somaticRefSampleBqrPlot());
        this.sageSomaticTumorSampleBqrPlot = new InputDownloadCommand(sageSomaticOutput.somaticTumorSampleBqrPlot());
        LinxSomaticOutputLocations linxSomaticOutputLocations = linxSomaticOutput.linxOutputLocations();
        this.linxSomaticOutputDir = new InputDownloadCommand(linxSomaticOutputLocations.outputDirectory(), LOCAL_LINX_SOMATIC_DIR);
        this.linxGermlineDataDir =
                new InputDownloadCommand(linxGermlineOutput.linxOutputLocations().outputDirectory(), LOCAL_LINX_GERMLINE_DIR);
        this.chordPredictionTxt = new InputDownloadCommand(chordOutput.chordOutputLocations().predictions());
        CuppaOutputLocations cuppaOutputLocations = cuppaOutput.cuppaOutputLocations();
        this.cuppaVisData = new InputDownloadCommand(cuppaOutputLocations.visData());
        this.cuppaVisPlot = new InputDownloadCommand(cuppaOutputLocations.visPlot());
        this.cuppaPredSumm = new InputDownloadCommand(cuppaOutputLocations.predSumm());
        this.peachGenotypeTsv = new InputDownloadCommand(peachOutput.genotypes());
        this.sigsAllocationTsv = new InputDownloadCommand(sigsOutput.allocationTsv());
        this.annotatedVirusTsv = new InputDownloadCommand(virusOutput.virusAnnotations());
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
                cuppaVisData,
                cuppaVisPlot,
                cuppaPredSumm,
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
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        if (includeGermline) {
            List<String> orangeArguments = new ArrayList<>();
            orangeArguments.addAll(commonArguments());
            orangeArguments.addAll(tumorArguments(metadata));
            return buildCommands(orangeArguments);
        } else {
            return Stage.disabled();
        }
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> orangeArguments = new ArrayList<>();
        orangeArguments.addAll(commonArguments());
        orangeArguments.addAll(tumorArguments(metadata));
        orangeArguments.addAll(germlineArguments(metadata));
        if (!includeGermline) {
            orangeArguments.add("-convert_germline_to_somatic");
        }
        return buildCommands(orangeArguments);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.orange(bash, resultsDirectory, namespace());
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
                    new AddDatatype(DataType.ORANGE_OUTPUT_PDF,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), orangePdf)));
        } else {
            return List.of();
        }
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }

    @NotNull
    private List<String> commonArguments() {
        final String experimentType = isTargeted ? "PANEL" : "WGS";
        return Lists.newArrayList("-output_dir",
                VmDirectories.OUTPUT,
                "-experiment_type",
                experimentType,
                "-ref_genome_version",
                resourceFiles.version().numeric(),
                "-doid_json",
                resourceFiles.doidJson(),
                "-sample_data_dir",
                VmDirectories.INPUT,
                "-purple_dir",
                purpleOutputDir.getLocalTargetPath(),
                "-purple_plot_dir",
                purpleOutputDir.getLocalTargetPath() + "/plot",
                "-lilac_dir",
                VmDirectories.INPUT,
                "-pipeline_version_file",
                PIPELINE_VERSION_FILE_PATH,
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
                "-signatures_etiology_tsv",
                resourceFiles.signaturesEtiology(),
                "-add_disclaimer");
    }

    @NotNull
    private List<String> tumorArguments(final SomaticRunMetadata metadata) {
        final List<String> primaryTumorDoids = metadata.tumor().primaryTumorDoids();
        final String primaryTumorDoidsString = "\"" + String.join(";", primaryTumorDoids) + "\"";

        final List<String> arguments = Lists.newArrayList("-tumor_sample_id",
                metadata.tumor().sampleName(),
                "-primary_tumor_doids",
                primaryTumorDoidsString,
                "-tumor_metrics_dir",
                VmDirectories.INPUT,
                "-linx_plot_dir",
                getLinxPlotDir(),
                "-linx_dir",
                linxSomaticOutputDir.getLocalTargetPath(),
                "-sage_dir",
                VmDirectories.INPUT);

        final Optional<LocalDate> optionalSamplingDate = metadata.tumor().samplingDate();
        if (optionalSamplingDate.isPresent()) {
            arguments.add("-sampling_date");
            arguments.add(DateTimeFormatter.ofPattern("yyMMdd").format(optionalSamplingDate.get()));
        }
        return arguments;
    }

    @NotNull
    private List<String> germlineArguments(final SomaticRunMetadata metadata) {
        return List.of("-reference_sample_id",
                metadata.reference().sampleName(),
                "-ref_metrics_dir",
                VmDirectories.INPUT,
                "-linx_germline_dir",
                linxGermlineDataDir.getLocalTargetPath());
    }

    @NotNull
    private List<BashCommand> buildCommands(final List<String> orangeArguments) {
        final String pipelineVersion = VersionUtils.pipelineMajorMinorVersion();

        JavaJarCommand orangeJarCommand = JavaCommandFactory.javaJarCommand(ORANGE, orangeArguments);

        return List.of(new MkDirCommand(getLinxPlotDir()),
                () -> "echo '" + pipelineVersion + "' | tee " + PIPELINE_VERSION_FILE_PATH,
                orangeJarCommand);
    }

    @NotNull
    private String getLinxPlotDir() {
        return linxSomaticOutputDir.getLocalTargetPath() + "/plot";
    }
}
