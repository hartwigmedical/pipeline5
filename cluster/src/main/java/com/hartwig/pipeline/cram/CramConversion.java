package com.hartwig.pipeline.cram;

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
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;

import java.util.Collections;
import java.util.List;

public class CramConversion implements Stage<CramOutput, SingleSampleRunMetadata> {
    public static final String NAMESPACE = "cram";
    static final int NUMBER_OF_CORES = 6;

    private final InputDownload bamDownload;

    public CramConversion(final AlignmentOutput alignmentOutput) {
        bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
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
        return new CramAndValidateCommands(bamDownload.getLocalTargetPath(),
                VmDirectories.outputFile(CramOutput.cramFile(metadata.sampleName()))).commands();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(BashStartupScript bash, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("cram")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(NUMBER_OF_CORES, 6))
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public CramOutput output(SingleSampleRunMetadata metadata, PipelineStatus jobStatus, RuntimeBucket bucket, ResultsDirectory resultsDirectory) {
        String cram = CramOutput.cramFile(metadata.sampleName());
        String crai = CramOutput.craiFile(metadata.sampleName());
        Folder folder = Folder.from(metadata);
        return CramOutput.builder()
                .status(jobStatus)
                .addReportComponents(
                        new RunLogComponent(bucket, NAMESPACE, folder, resultsDirectory),
                        new StartupScriptComponent(bucket, NAMESPACE, folder),
                        new SingleFileComponent(bucket, NAMESPACE, folder, cram, cram, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, folder, crai, crai, resultsDirectory))
                .build();
    }

    @Override
    public CramOutput skippedOutput(SingleSampleRunMetadata metadata) {
        throw new IllegalStateException("CRAM conversion cannot be skipped.");
    }

    @Override
    public boolean shouldRun(Arguments arguments) {
        return true;
    }
}
