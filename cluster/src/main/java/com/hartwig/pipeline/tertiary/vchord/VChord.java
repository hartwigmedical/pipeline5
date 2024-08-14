package com.hartwig.pipeline.tertiary.vchord;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tools.HmfTool;

@Namespace(VChord.NAMESPACE)
public class VChord implements Stage<VChordOutput, SomaticRunMetadata>
{

    public static final String NAMESPACE = "vchord";

    private final InputDownloadCommand purpleOutputDirDownload;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public VChord(final PurpleOutput purpleOutput,
            final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        purpleOutputDirDownload = new InputDownloadCommand(purpleOutputLocations.outputDirectory());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(purpleOutputDirDownload);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata)
    {
        List<String> arguments = new ArrayList<>();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        addCommonArguments(arguments);

        return formCommand(arguments);
    }

    private List<BashCommand> formCommand(final List<String> arguments)
    {
        List<BashCommand> commands = new ArrayList<>();
        commands.add(JavaCommandFactory.javaJarCommand(HmfTool.VCHORD, arguments));
        return commands;
    }

    private void addCommonArguments(final List<String> arguments) {
        arguments.add(String.format("-purple %s", purpleOutputDirDownload.getLocalTargetPath()));
        arguments.add(String.format("-model %s", resourceFiles.vChordModel()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.vchord(startupScript, resultsDirectory);
    }

    @Override
    public VChordOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        ImmutableVChordOutputLocations.Builder outputLocationsBuilder = VChordOutputLocations.builder();

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticVChordPrediction = vChordPrediction(tumorSampleName);

            outputLocationsBuilder.vChordPrediction(
                    GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVChordPrediction)));
        });
        
        return ImmutableVChordOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public VChordOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VChordOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VChordOutput persistedOutput(final SomaticRunMetadata metadata) {

        final String tumorSampleName = metadata.tumor().sampleName();
        final String somaticVChordPrediction = vChordPrediction(tumorSampleName);

        GoogleStorageLocation somaticVChordPredictionLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.VCHORD_PREDICTION,
                somaticVChordPrediction);

        ImmutableVChordOutputLocations.Builder outputLocationsBuilder = VChordOutputLocations.builder()
                .vChordPrediction(somaticVChordPredictionLocation);

        return VChordOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .addAllDatatypes(addDatatypes(metadata))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        List<AddDatatype> datatypes = new ArrayList<>();

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            final String somaticVChordPrediction = vChordPrediction(tumorSampleName);

            datatypes.add(new AddDatatype(DataType.VCHORD_PREDICTION,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticVChordPrediction)));
        });

        return datatypes;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && arguments.useTargetRegions();
    }

    private GoogleStorageLocation persistedOrDefault(final String sample, final String set, final String bucket,
            final DataType dataType, final String fileName) {
        return persistedDataset.path(sample, dataType)
                .orElse(GoogleStorageLocation.of(bucket, PersistedLocations.blobForSet(set, namespace(), fileName)));
    }

    private static String vChordPrediction(final String sample) {
        return sample + ".vchord.prediction.tsv";
    }
}
