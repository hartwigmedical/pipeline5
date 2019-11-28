package com.hartwig.batch.operations;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.ImmutableInputFileDescriptor;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import java.io.File;

import static java.lang.String.format;

public class FlagstatGenerator implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputFileDescriptor descriptor, final RuntimeBucket bucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        InputFileDescriptor existingFlagstat = ImmutableInputFileDescriptor.builder().from(descriptor)
                .remoteFilename(descriptor.remoteFilename().replaceAll("\\.bam$", ".flagstat")).build();
        String localCopyOfOriginalFlagstat = format("%s/%s", VmDirectories.OUTPUT, new File(existingFlagstat.remoteFilename()).getName());
        String outputFile = VmDirectories.outputFile(new File(descriptor.remoteFilename()).getName().replaceAll("\\.bam$", ".batch.flagstat"));
        String localInput = format("%s/%s", VmDirectories.INPUT, new File(descriptor.remoteFilename()).getName());
        startupScript.addCommand(() -> descriptor.toCommandForm(localInput));
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
        return VirtualMachineJobDefinition.batchFlagstat(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("Flagstat", "Generate flagstat file for each input");
    }
}
