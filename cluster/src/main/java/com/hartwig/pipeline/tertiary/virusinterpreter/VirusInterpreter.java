package com.hartwig.pipeline.tertiary.virusinterpreter;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.virusbreakend.VirusBreakend;
import com.hartwig.pipeline.tertiary.virusbreakend.VirusBreakendOutput;

public class VirusInterpreter implements Stage<VirusInterpreterOutput, SomaticRunMetadata> {

    private final InputDownload virusBreakendSummary;
    private final ResourceFiles resourceFiles;

    public VirusInterpreter(final VirusBreakendOutput virusBreakendOutput, final ResourceFiles resourceFiles) {
        this.virusBreakendSummary = new InputDownload(virusBreakendOutput.outputLocations().summary());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public String namespace() {
        return VirusBreakend.NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(virusBreakendSummary);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new VirusInterpreterCommand(metadata.tumor().sampleName(),
                virusBreakendSummary.getLocalTargetPath(),
                resourceFiles.virusInterpreterTaxonomyDb(),
                resourceFiles.virusInterpretation(),
                resourceFiles.virusBlacklist(),
                VmDirectories.OUTPUT));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(namespace())
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 12))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public VirusInterpreterOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return VirusInterpreterOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .build();
    }

    @Override
    public VirusInterpreterOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusInterpreterOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VirusInterpreterOutput persistedOutput(final SomaticRunMetadata metadata) {
        return VirusInterpreterOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
