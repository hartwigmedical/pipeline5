package com.hartwig.batch.operations;

import com.google.common.collect.ImmutableList;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import java.io.File;
import java.util.List;

import static com.hartwig.pipeline.calling.command.VersionedToolCommand.sambamba;
import static java.lang.String.format;

public class Bam2Fastq implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(InputFileDescriptor descriptor, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {
        String localCopyOfBam = format("%s/%s", VmDirectories.OUTPUT, new File(descriptor.remoteFilename()).getName());
        startupScript.addCommand(() -> descriptor.toCommandForm(localCopyOfBam));
        startupScript.addCommand(new PipeCommands(sambamba("view", "-H", localCopyOfBam),
                () -> "grep ^@RG",
                () -> "grep -cP \"_L00[1-8]_\""
        ));
        List<String> picargs = ImmutableList.of("SamToFastq", "ODIR=" + VmDirectories.OUTPUT, "OPRG=true", "RGT=ID", "NON_PF=true", "RC=true", "I=" + localCopyOfBam);
        startupScript.addCommand(new JavaJarCommand("picard", "2.18.27", "picard.jar", "12G", picargs));
        startupScript.addCommand(() -> format("rename 's/(.+)_(.+)_(.+)_(.+)_(.+)__(.+)\\.fastq/$1_$2_$3_$4_R$6_$5.fastq/' %s/*.fastq", VmDirectories.OUTPUT));
        startupScript.addCommand(() -> format("pigz %s/*.fastq", VmDirectories.OUTPUT));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "bam2fastq"), executionFlags));
        return VirtualMachineJobDefinition.batchBam2Fastq(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("Bam2Fastq", "Convert BAMs back to FASTQs");
    }
}
