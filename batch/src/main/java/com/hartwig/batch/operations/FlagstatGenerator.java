package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class FlagstatGenerator implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        InputFileDescriptor input = inputs.get();
        InputFileDescriptor existingFlagstat = InputFileDescriptor.from(input).withInputValue(input.inputValue().replaceAll("\\.bam$", ".flagstat"));
        String localCopyOfOriginalFlagstat = format("%s/%s", VmDirectories.OUTPUT, new File(existingFlagstat.inputValue()).getName());
        String outputFile = VmDirectories.outputFile(new File(input.inputValue()).getName().replaceAll("\\.bam$", ".batch.flagstat"));
        String localInput = format("%s/%s", VmDirectories.INPUT, new File(input.inputValue()).getName());
        startupScript.addCommand(() -> input.toCommandForm(localInput));
        startupScript.addCommand(() -> existingFlagstat.toCommandForm(localCopyOfOriginalFlagstat));
        startupScript.addCommand(new PipeCommands(new VersionedToolCommand("sambamba",
                "sambamba",
                Versions.SAMBAMBA,
                "flagstat",
                "-t",
                Bash.allCpus(),
                localInput), () -> "tee " + outputFile));
        startupScript.addCommand(() -> format("diff %s %s", localCopyOfOriginalFlagstat, outputFile));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "flagstat"), executionFlags));
        return VirtualMachineJobDefinition.builder().name("flagstat").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 6)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("Flagstat", "Generate flagstat file for each inputs",
                OperationDescriptor.InputType.FLAT);
    }
}
