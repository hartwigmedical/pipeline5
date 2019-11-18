package com.hartwig.batch.operations;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class FlagstatGenerator implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputFileDescriptor descriptor, final RuntimeBucket bucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        String outputFile = VmDirectories.outputFile(new File(descriptor.remoteFilename()).getName().replaceAll("$", ".batch.flagstat"));
        String localInput = String.format("%s/%s", VmDirectories.INPUT, new File(descriptor.remoteFilename()).getName());
        startupScript.addCommand(() -> descriptor.toCommandForm(localInput));
        startupScript.addCommand(new PipeCommands(new VersionedToolCommand("sambamba",
                "sambamba",
                Versions.SAMBAMBA,
                "flagstat",
                "-t",
                Bash.allCpus(),
                localInput), () -> "tee " + outputFile));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "flagstat"), executionFlags));
        return VirtualMachineJobDefinition.batchFlagstat(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("Flagstat", "Generate flagstat file for each input");
    }
}
