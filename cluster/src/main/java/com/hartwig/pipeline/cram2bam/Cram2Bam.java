package com.hartwig.pipeline.cram2bam;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

@Namespace(Cram2Bam.NAMESPACE)
public class Cram2Bam implements Stage<AlignmentOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "cram2bam";

    private final InputDownload bamDownload;
    private final SingleSampleRunMetadata.SampleType sampleType;

    public Cram2Bam(final GoogleStorageLocation bamLocation, final SingleSampleRunMetadata.SampleType sampleType) {
        this.bamDownload = new InputDownload(bamLocation);
        this.sampleType = sampleType;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(bamDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata) {
        String outputBam = VmDirectories.OUTPUT + "/" + FileTypes.bam(metadata.sampleName());
        return List.of(new SamtoolsCommand("view", "-O", "bam", "-o", outputBam, "-@", Bash.allCpus(), bamDownload.getLocalTargetPath()),
                new SamtoolsCommand("index", outputBam));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .workingDiskSpaceGb(sampleType.equals(SingleSampleRunMetadata.SampleType.REFERENCE) ? 650 : 950)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(32, 32))
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .name(namespace())
                .build();
    }

    @Override
    public AlignmentOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String bam = FileTypes.bam(metadata.sampleName());
        return AlignmentOutput.builder()
                .name(namespace())
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
