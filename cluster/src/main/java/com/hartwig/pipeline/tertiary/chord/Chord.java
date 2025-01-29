package com.hartwig.pipeline.tertiary.chord;

import static java.lang.String.format;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.CHORD;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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

import org.jetbrains.annotations.NotNull;

@Namespace(Chord.NAMESPACE)
public class Chord implements Stage<ChordOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "chord";
    public static final String PREDICTION_TSV = ".chord.prediction.tsv";
    public static final String MUTATION_CONTEXTS_TSV = ".chord.mutation_contexts.tsv";

    private final InputDownloadCommand purpleStructuralVcfDownload;
    private final InputDownloadCommand purpleSomaticVcfDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Chord(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles,
            final PersistedDataset persistedDataset) {
        this.purpleStructuralVcfDownload = initialiseOptionalLocation(purpleOutput.outputLocations().structuralVariants());
        this.purpleSomaticVcfDownload = initialiseOptionalLocation(purpleOutput.outputLocations().somaticVariants());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(purpleStructuralVcfDownload, purpleSomaticVcfDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return chordCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return chordCommands(metadata);
    }

    public List<BashCommand> chordCommands(final SomaticRunMetadata metadata) {

        List<String> chordArguments = Lists.newArrayList(format("-sample %s", metadata.tumor().sampleName()),
                format("-ref_genome %s", resourceFiles.refGenomeFile()),
                format("-purple_dir %s", VmDirectories.INPUT),
                format("-output_dir %s", VmDirectories.OUTPUT),
                "-log_level DEBUG"
        );

        List<BashCommand> chordCommands = Lists.newArrayList(JavaCommandFactory.javaJarCommand(CHORD, chordArguments));

        return chordCommands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.chord(bash, resultsDirectory);
    }

    @Override
    public ChordOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return ChordOutput.builder()
                .status(jobStatus)
                .maybeChordOutputLocations(ChordOutputLocations.builder()
                        .predictions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(chordPredictionTxt(metadata))))
                        .signatures(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(chordSignaturesTxt(metadata))))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @NotNull
    private String chordPredictionTxt(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PREDICTION_TSV;
    }

    @NotNull
    private String chordSignaturesTxt(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + MUTATION_CONTEXTS_TSV;
    }

    @Override
    public ChordOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public ChordOutput persistedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeChordOutputLocations(ChordOutputLocations.builder()
                        .predictions(persistedDataset.path(metadata.tumor().sampleName(), DataType.CHORD_PREDICTION)
                                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                        PersistedLocations.blobForSet(metadata.set(), namespace(), chordPredictionTxt(metadata)))))
                        .signatures(persistedDataset.path(metadata.tumor().sampleName(), DataType.CHORD_MUTATION_CONTEXTS)
                                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                        PersistedLocations.blobForSet(metadata.set(), namespace(), chordSignaturesTxt(metadata)))))
                        .build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {

        return List.of(new AddDatatype(DataType.CHORD_PREDICTION,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), chordPredictionTxt(metadata))),
                new AddDatatype(DataType.CHORD_MUTATION_CONTEXTS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), chordSignaturesTxt(metadata))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow() && !arguments.useTargetRegions();
    }
}
