package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.cram.CramAndValidateCommands;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SamtoolsBamToCram implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        InputFileDescriptor input = inputs.get();
        String outputFile = VmDirectories.outputFile(new File(input.inputValue()).getName().replaceAll("\\.bam$", ".cram"));
        String localInput = format("%s/%s", VmDirectories.INPUT, new File(input.inputValue()).getName());
        startupScript.addCommand(() -> input.toCommandForm(localInput));

        startupScript.addCommands(new CramAndValidateCommands(localInput, outputFile).commands());
        startupScript.addCommand(new MvCommand("/data/output/*.bam", "/data/tmp"));
        startupScript.addCommand(new MvCommand("/data/output/*.bam.flagstat", "/data/tmp"));

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "samtools"), executionFlags));
        return VirtualMachineJobDefinition.builder().name("samtoolscram").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(650)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(6, 6)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SamtoolsBamToCram", "Produce a CRAM file from each input BAM",
                OperationDescriptor.InputType.FLAT);
    }
}
