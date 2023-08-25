package com.hartwig.pipeline.metrics;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.*;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.input.SingleSampleRunMetadata;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile.custom;

@Namespace(BamMetrics.NAMESPACE)
public class BamMetrics implements Stage<BamMetricsOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "bam_metrics";

    private final ResourceFiles resourceFiles;
    private final InputDownloadCommand bamDownload;
    private final InputDownloadCommand bamBaiDownload;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public BamMetrics(final ResourceFiles resourceFiles, final AlignmentOutput alignmentOutput, final PersistedDataset persistedDataset,
                      final Arguments arguments) {
        this.resourceFiles = resourceFiles;
        bamDownload = new InputDownloadCommand(alignmentOutput.alignments());
        bamBaiDownload = new InputDownloadCommand(alignmentOutput.alignments().transform(FileTypes::toAlignmentIndex));
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runBamMetrics();
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(bamDownload, bamBaiDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    public List<BashCommand> bamMetricsCommands(final SingleSampleRunMetadata metadata) {
        ArrayList<BashCommand> bashCommands = new ArrayList<>();

        bashCommands.add(new BamMetricsCommand(
                metadata.sampleName(),
                bamDownload.getLocalTargetPath(),
                resourceFiles,
                VmDirectories.OUTPUT,
                Bash.allCpus(),
                arguments.useTargetRegions() ? resourceFiles.targetRegionsBed() : null));

        return bashCommands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript script, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("bam-metrics")
                .startupCommand(script)
                .performanceProfile(custom(16, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public BamMetricsOutput output(final SingleSampleRunMetadata metadata, final ComputeEngineStatus jobStatus, final RuntimeBucket bucket,
                                   final ResultsDirectory resultsDirectory) {
        String outputFile = BamMetricsOutput.outputFile(metadata.sampleName());
        return BamMetricsOutput.builder()
                .status(jobStatus)
                .sample(metadata.sampleName())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeMetricsOutputFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.from(metadata)))
                .addReportComponents(new SingleFileComponent(bucket,
                        namespace(),
                        Folder.from(metadata),
                        outputFile,
                        outputFile,
                        resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public BamMetricsOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder().sample(metadata.sampleName()).status(ComputeEngineStatus.SKIPPED).build();
    }

    @Override
    public BamMetricsOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder()
                .status(ComputeEngineStatus.PERSISTED)
                .sample(metadata.sampleName())
                .maybeMetricsOutputFile(persistedDataset.path(metadata.sampleName(), DataType.WGSMETRICS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSingle(metadata.set(),
                                        metadata.sampleName(),
                                        namespace(),
                                        BamMetricsOutput.outputFile(metadata.sampleName())))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SingleSampleRunMetadata metadata) {
        return Collections.singletonList(new AddDatatype(DataType.WGSMETRICS,
                metadata.barcode(),
                new ArchivePath(Folder.from(metadata), namespace(), BamMetricsOutput.outputFile(metadata.sampleName()))));
    }
}
