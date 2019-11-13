package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
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
    public VirtualMachineJobDefinition execute(final String input, final RuntimeBucket bucket, final String instanceId) {
        String outputFile = VmDirectories.outputFile(new File(input).getName().replaceAll("\\.bam$", ".flagstat"));
        String localInput = String.format("%s/%s", VmDirectories.INPUT, new File(input).getName());
        RuntimeFiles executionFlags = RuntimeFiles.of(instanceId);
        BashStartupScript startupScript = BashStartupScript.of(bucket.name(), executionFlags);
        startupScript.addCommand(() -> format("gsutil cp %s %s", input, localInput));
        startupScript.addCommand(new PipeCommands(new VersionedToolCommand("sambamba",
                "sambamba",
                Versions.SAMBAMBA,
                "flagstat",
                "-t",
                Bash.allCpus(),
                localInput), () -> "tee " + outputFile));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "flagstat"), executionFlags));
        return VirtualMachineJobDefinition.cramMigration(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("Flagstat", "Generate flagstat file for each input");
    }
}
