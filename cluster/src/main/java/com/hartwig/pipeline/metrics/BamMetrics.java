package com.hartwig.pipeline.metrics;

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
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class BamMetrics implements Stage<BamMetricsOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "bam_metrics";

    final ResourceFiles resourceFiles;
    private final InputDownload bamDownload;
    private final PersistedDataset persistedDataset;

    public BamMetrics(final ResourceFiles resourceFiles, final AlignmentOutput alignmentOutput, final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        this.persistedDataset = persistedDataset;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runBamMetrics();
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
    public List<BashCommand> commands(SingleSampleRunMetadata metadata) {
        return Collections.singletonList(new BamMetricsCommand(bamDownload.getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                VmDirectories.OUTPUT + "/" + BamMetricsOutput.outputFile(metadata.sampleName())));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript script, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.bamMetrics(script, resultsDirectory);
    }

    @Override
    public BamMetricsOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String outputFile = BamMetricsOutput.outputFile(metadata.sampleName());
        return BamMetricsOutput.builder()
                .status(jobStatus)
                .sample(metadata.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.from(metadata)))
                .addReportComponents(new SingleFileComponent(bucket,
                        namespace(),
                        Folder.from(metadata),
                        outputFile,
                        outputFile,
                        resultsDirectory))
                .addFurtherOperations(new AddDatatypeToFile(DataType.WGSMETRICS,
                        Folder.from(metadata),
                        namespace(),
                        outputFile,
                        metadata.barcode()))
                .build();
    }

    @Override
    public BamMetricsOutput skippedOutput(SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public BamMetricsOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        String wgsMetricsPath = persistedDataset.file(metadata, DataType.WGSMETRICS)
                .orElse(PersistedLocations.blobForSingle(metadata.set(),
                        metadata.sampleName(),
                        namespace(),
                        BamMetricsOutput.outputFile(metadata.sampleName())));
        return BamMetricsOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .sample(metadata.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(metadata.bucket(), wgsMetricsPath))
                .build();
    }
}
