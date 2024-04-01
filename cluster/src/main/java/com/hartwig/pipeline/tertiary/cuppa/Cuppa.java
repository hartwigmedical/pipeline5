package com.hartwig.pipeline.tertiary.cuppa;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.unix.SubShellCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.execution.vm.python.Python3ModuleCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static com.hartwig.pipeline.tools.HmfTool.CUPPA;
import static java.lang.String.format;

@Namespace(Cuppa.NAMESPACE)
public class Cuppa implements Stage<CuppaOutput, SomaticRunMetadata> {
    public static final String CUPPA_DATA_PREP = "com.hartwig.hmftools.cup.prep.CuppaDataPrep";

    public static final String CUPPA_VIS_DATA = ".cuppa.vis_data.tsv";
    public static final String CUPPA_VIS_PLOT = ".cuppa.vis.png";
    public static final String CUPPA_PRED_SUMM = ".cuppa.pred_summ.tsv";
    public static final String NAMESPACE = "cuppa";

    private final InputDownloadCommand purpleOutputDirectory;
    private final InputDownloadCommand linxOutputDirectory;
    private final InputDownloadCommand virusInterpreterAnnotations;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public Cuppa(final PurpleOutput purpleOutput, final LinxSomaticOutput linxOutput, final VirusInterpreterOutput virusInterpreterOutput,
                 final ResourceFiles resourceFiles, final PersistedDataset persistedDataset, final Arguments arguments) {
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purpleOutputDirectory = new InputDownloadCommand(purpleOutputLocations.outputDirectory());
        this.linxOutputDirectory = new InputDownloadCommand(linxOutput.linxOutputLocations().outputDirectory());
        this.virusInterpreterAnnotations = new InputDownloadCommand(virusInterpreterOutput.virusAnnotations());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleOutputDirectory, linxOutputDirectory, virusInterpreterAnnotations);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return cuppaCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return cuppaCommands(metadata);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.cuppa(bash, resultsDirectory);
    }

    @Override
    public CuppaOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return CuppaOutput.builder()
                .status(jobStatus)
                .maybeCuppaOutputLocations(CuppaOutputLocations.builder()
                        .visData(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(cuppaVisData(metadata))))
                        .visPlot(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(cuppaVisPlot(metadata))))
                        .predSumm(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(cuppaPredSumm(metadata))))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public CuppaOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CuppaOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public CuppaOutput persistedOutput(final SomaticRunMetadata metadata) {
        final String visData = cuppaVisData(metadata);
        final String visPlot = cuppaVisPlot(metadata);
        final String predSumm = cuppaPredSumm(metadata);
        return CuppaOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeCuppaOutputLocations(CuppaOutputLocations.builder()
                        .visData(persistedDataset.path(metadata.tumor().sampleName(), DataType.CUPPA_VIS_DATA)
                                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                        PersistedLocations.blobForSet(metadata.set(), namespace(), visData))))
                        .visPlot(persistedDataset.path(metadata.tumor().sampleName(), DataType.CUPPA_VIS_PLOT)
                                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                        PersistedLocations.blobForSet(metadata.set(), namespace(), visPlot))))
                        .predSumm(persistedDataset.path(metadata.tumor().sampleName(), DataType.CUPPA_PRED_SUMM)
                                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                        PersistedLocations.blobForSet(metadata.set(), namespace(), predSumm))))
                        .build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.CUPPA_VIS_DATA,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), cuppaVisData(metadata))),
                new AddDatatype(DataType.CUPPA_VIS_PLOT,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), cuppaVisPlot(metadata))),
                new AddDatatype(DataType.CUPPA_PRED_SUMM,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), cuppaPredSumm(metadata))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary() && !arguments.useTargetRegions();
    }

    @NotNull
    private List<BashCommand> cuppaCommands(final SomaticRunMetadata metadata) {
        List<String> cuppaArguments = Lists.newArrayList(format("-sample %s", metadata.tumor().sampleName()),
                "-categories DNA",
                format("-ref_genome_version %s", resourceFiles.version().toString()),
                format("-sample_data_dir %s", linxOutputDirectory.getLocalTargetPath()),
                format("-output_dir %s", VmDirectories.OUTPUT));

        List<BashCommand> cuppaCommands = Lists.newArrayList(JavaCommandFactory.javaClassCommand(CUPPA, CUPPA_DATA_PREP, cuppaArguments));

        String cuppaInputFeaturesFile = VmDirectories.outputFile(format("%s.cuppa_data.tsv.gz", metadata.tumor().sampleName()));
        String cuppaClassifierFile = resourceFiles.cuppaClassifier();
        String cuppaCvPredictionsFile = resourceFiles.cuppaCvPredictions();

        List<String> pycuppaPredictArguments = Lists.newArrayList(
                format("--classifier_path %s", cuppaClassifierFile),
                format("--features_path %s", cuppaInputFeaturesFile),
                format("--output_dir %s", VmDirectories.OUTPUT),
                format("--sample_id %s", metadata.tumor().sampleName()));

        if (arguments.usePrivateResources()) {
            pycuppaPredictArguments.add(format("--cv_predictions_path %s", cuppaCvPredictionsFile));
        }

        cuppaCommands.add(new SubShellCommand(new Python3ModuleCommand("pycuppa",
                CUPPA.runVersion(),
                "cuppa.predict",
                pycuppaPredictArguments)));

        return cuppaCommands;
    }

    private String cuppaVisData(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + CUPPA_VIS_DATA;
    }

    private String cuppaVisPlot(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + CUPPA_VIS_PLOT;
    }

    private String cuppaPredSumm(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + CUPPA_PRED_SUMM;
    }
}