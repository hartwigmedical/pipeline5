package com.hartwig.pipeline.cram;

import static java.lang.String.format;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.LinkFileToSample;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class CramConversion implements Stage<CramOutput, SingleSampleRunMetadata> {
    public static final String NAMESPACE = "cram";
    static final int NUMBER_OF_CORES = 6;

    private final InputDownload bamDownload;
    private final String outputCram;
    private final ResourceFiles resourceFiles;

    public CramConversion(final AlignmentOutput alignmentOutput, ResourceFiles resourceFiles) {
        bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        outputCram = VmDirectories.outputFile(alignmentOutput.sample() + ".cram");
        this.resourceFiles = resourceFiles;
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
        return new CramAndValidateCommands(bamDownload.getLocalTargetPath(), outputCram, resourceFiles).commands();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(BashStartupScript bash, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("cram")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(NUMBER_OF_CORES, 6))
                .workingDiskSpaceGb(650)
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public CramOutput output(SingleSampleRunMetadata metadata, PipelineStatus jobStatus, RuntimeBucket bucket, ResultsDirectory resultsDirectory) {
        String cram = new File(outputCram).getName();
        String crai = CramOutput.craiFile(cram);
        Folder folder = Folder.from(metadata);

        String fullCram = format("%s%s/%s", folder.name(), NAMESPACE, cram);
        String fullCrai = format("%s%s/%s", folder.name(), NAMESPACE, crai);

        return CramOutput.builder()
                .status(jobStatus)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, folder, resultsDirectory),
                        new StartupScriptComponent(bucket, NAMESPACE, folder),
                        new SingleFileComponent(bucket, NAMESPACE, folder, cram, cram, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, folder, crai, crai, resultsDirectory))
                .addFurtherOperations(new LinkFileToSample(fullCram, metadata.entityId()),
                        new LinkFileToSample(fullCrai, metadata.entityId()),
                        new AddDatatypeToFile(fullCram, "reads"),
                        new AddDatatypeToFile(fullCrai, "reads"))
                .build();
    }

    @Override
    public CramOutput skippedOutput(SingleSampleRunMetadata metadata) {
        return CramOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(Arguments arguments) {
        return arguments.outputCram();
    }
}
