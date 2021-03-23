package com.hartwig.pipeline.tertiary.chord;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Chord implements Stage<ChordOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "chord";
    public static final String PREDICTION_TXT = "_chord_prediction.txt";

    private final RefGenomeVersion refGenomeVersion;
    private final InputDownload purpleStructuralVcfDownload;
    private final InputDownload purpleSomaticVcfDownload;

    public Chord(final RefGenomeVersion refGenomeVersion, final PurpleOutput purpleOutput) {
        this.refGenomeVersion = refGenomeVersion;
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.outputLocations().structuralVcf());
        purpleSomaticVcfDownload = new InputDownload(purpleOutput.outputLocations().somaticVcf());
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
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new ChordExtractSigPredictHRD(metadata.tumor().sampleName(),
                purpleSomaticVcfDownload.getLocalTargetPath(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                refGenomeVersion));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.chord(bash, resultsDirectory);
    }

    @Override
    public ChordOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String chordPredictionTxt = metadata.tumor().sampleName() + PREDICTION_TXT;
        return ChordOutput.builder()
                .status(jobStatus)
                .maybePredictions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(chordPredictionTxt)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.CHORD_PREDICTION,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), chordPredictionTxt)))
                .build();
    }

    @Override
    public ChordOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public ChordOutput persistedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
