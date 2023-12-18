package com.hartwig.pipeline.tertiary.chord;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

@Namespace(Chord.NAMESPACE)
public class Chord implements Stage<ChordOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "chord";
    public static final String PREDICTION_TXT = "_chord_prediction.txt";

    private final RefGenomeVersion refGenomeVersion;
    private final InputDownloadCommand purpleStructuralVcfDownload;
    private final InputDownloadCommand purpleSomaticVcfDownload;
    private final PersistedDataset persistedDataset;

    public Chord(final RefGenomeVersion refGenomeVersion, final PurpleOutput purpleOutput, final PersistedDataset persistedDataset) {
        this.refGenomeVersion = refGenomeVersion;
        this.purpleStructuralVcfDownload = initialiseOptionalLocation(purpleOutput.outputLocations().structuralVariants());
        this.purpleSomaticVcfDownload = initialiseOptionalLocation(purpleOutput.outputLocations().somaticVariants());
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
        return Collections.singletonList(new ChordExtractSigPredictHRD(metadata.tumor().sampleName(),
                purpleSomaticVcfDownload.getLocalTargetPath(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                refGenomeVersion));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.chord(bash, resultsDirectory);
    }

    @Override
    public ChordOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String chordPredictionTxt = chordPredictionTxt(metadata);
        return ChordOutput.builder()
                .status(jobStatus)
                .maybePredictions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(chordPredictionTxt)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @NotNull
    private String chordPredictionTxt(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PREDICTION_TXT;
    }

    @Override
    public ChordOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public ChordOutput persistedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybePredictions(persistedDataset.path(metadata.tumor().sampleName(), DataType.CHORD_PREDICTION)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), chordPredictionTxt(metadata)))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new AddDatatype(DataType.CHORD_PREDICTION,
                metadata.barcode(),
                new ArchivePath(Folder.root(), namespace(), chordPredictionTxt(metadata))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow() && !arguments.useTargetRegions();
    }
}
