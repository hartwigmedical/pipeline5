package com.hartwig.pipeline.flagstat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class Flagstat implements Stage<FlagstatOutput, SingleSampleRunMetadata> {
    public static final String NAMESPACE = "flagstat";

    private final InputDownload bamDownload;
    private final PersistedDataset persistedDataset;

    public Flagstat(final AlignmentOutput alignmentOutput, final PersistedDataset persistedDataset) {
        bamDownload = new InputDownload(alignmentOutput.alignments());
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(bamDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata) {
        String outputFile = FlagstatOutput.outputFile(metadata.sampleName());
        return Collections.singletonList(new SubShellCommand(new SambambaFlagstatCommand(bamDownload.getLocalTargetPath(),
                VmDirectories.OUTPUT + "/" + outputFile)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.flagstat(bash, resultsDirectory);
    }

    @Override
    public FlagstatOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus status, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String outputFile = FlagstatOutput.outputFile(metadata.sampleName());
        return FlagstatOutput.builder()
                .status(status)
                .sample(metadata.sampleName())
                .maybeFlagstatOutputFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new RunLogComponent(bucket, Flagstat.NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from(metadata)))
                .addReportComponents(new SingleFileComponent(bucket,
                        Flagstat.NAMESPACE,
                        Folder.from(metadata),
                        outputFile,
                        outputFile,
                        resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.FLAGSTAT,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), Flagstat.NAMESPACE, outputFile)))
                .build();
    }

    @Override
    public FlagstatOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        String outputFile = FlagstatOutput.outputFile(metadata.sampleName());
        return FlagstatOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .sample(metadata.sampleName())
                .maybeFlagstatOutputFile(persistedDataset.path(metadata.sampleName(), DataType.FLAGSTAT)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSingle(metadata.set(), metadata.sampleName(), namespace(), outputFile))))
                .addDatatypes(new AddDatatype(DataType.FLAGSTAT,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), outputFile)))
                .build();
    }

    @Override
    public FlagstatOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        throw new IllegalStateException("Flagstat cannot be skipped.");
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return true;
    }
}
