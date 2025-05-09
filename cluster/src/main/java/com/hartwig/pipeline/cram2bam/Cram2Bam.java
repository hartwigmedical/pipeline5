package com.hartwig.pipeline.cram2bam;

import java.util.List;

import com.hartwig.computeengine.execution.vm.Bash;
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
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.ReduxFileLocator;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

@Namespace("cram2bam")
public class Cram2Bam implements Stage<AlignmentOutput, SingleSampleRunMetadata> {

    private final Arguments arguments;
    private final ReduxFileLocator reduxFileLocator;
    private final InputDownloadCommand bamDownload;
    private final SingleSampleRunMetadata.SampleType sampleType;

    public Cram2Bam(final Arguments arguments, final ReduxFileLocator reduxFileLocator, final GoogleStorageLocation bamLocation,
            final SingleSampleRunMetadata.SampleType sampleType) {
        this.arguments = arguments;
        this.reduxFileLocator = reduxFileLocator;
        this.bamDownload = new InputDownloadCommand(bamLocation);
        this.sampleType = sampleType;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(bamDownload);
    }

    @Override
    public String namespace() {
        return "cram2bam";
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata) {
        String outputBam = VmDirectories.OUTPUT + "/" + FileTypes.bam(metadata.sampleName());
        return List.of(new SamtoolsCommand("view", "-O", "bam", "-o", outputBam, "-@", Bash.allCpus(), bamDownload.getLocalTargetPath()),
                new SamtoolsCommand("index", outputBam));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.cram2Bam(bash, resultsDirectory, sampleType, namespace());
    }

    @Override
    public AlignmentOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String bam = FileTypes.bam(metadata.sampleName());
        var builder = AlignmentOutput.builder();

        if (!arguments.redoDuplicateMarking()) {
            builder.maybeJitterParams(reduxFileLocator.locateJitterParamsFile(metadata))
                    .maybeMsTable(reduxFileLocator.locateMsTableFile(metadata));
        }

        return builder.name(namespace())
                .status(jobStatus)
                .sample(metadata.sampleName())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeAlignments(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(bam)))
                .build();
    }

    @Override
    public AlignmentOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return AlignmentOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return true;
    }
}
