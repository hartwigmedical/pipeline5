package com.hartwig.pipeline.metrics;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class BamMetrics implements Stage<BamMetricsOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "bam_metrics";
    private final InputDownload bamDownload;

    public BamMetrics(final AlignmentOutput alignmentOutput) {
        bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runBamMetrics();
    }

    @Override
    public List<InputDownload> inputs() {
        return Collections.singletonList(bamDownload);
    }

    @Override
    public List<ResourceDownload> resources(Storage storage, String resourceBucket, RuntimeBucket runtimeBucket) {
        return Collections.singletonList(ResourceDownload.from(runtimeBucket,
                new Resource(storage,
                        resourceBucket,
                        ResourceNames.REFERENCE_GENOME,
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias()))));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(SingleSampleRunMetadata metadata, Map<String, ResourceDownload> resources) {
        return Collections.singletonList(new BamMetricsCommand(bamDownload.getLocalTargetPath(),
                resources.get(ResourceNames.REFERENCE_GENOME).find("fasta"),
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
                .build();
    }

    @Override
    public BamMetricsOutput skippedOutput(SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.SKIPPED).build();
    }
}
