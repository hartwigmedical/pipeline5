package com.hartwig.batch.operations;

import com.google.common.collect.ImmutableList;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import java.io.File;
import java.util.List;

import static java.lang.String.format;

public class Bam2Fastq implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {
        InputFileDescriptor descriptor = inputs.get();
        String localCopyOfBam = format("%s/%s", VmDirectories.OUTPUT, new File(descriptor.remoteFilename()).getName());
        startupScript.addCommand(() -> descriptor.toCommandForm(localCopyOfBam));
        startupScript.addCommand(new PipeCommands(new SambambaCommand("view", "-H", localCopyOfBam),
                () -> "grep ^@RG",
                () -> "grep -cP \"_L00[1-8]_\""
        ));
        List<String> picargs = ImmutableList.of("SamToFastq", "ODIR=" + VmDirectories.OUTPUT, "OPRG=true", "RGT=ID", "NON_PF=true", "RC=true", "I=" + localCopyOfBam);
        startupScript.addCommand(new JavaJarCommand("picard", "2.18.27", "picard.jar", "12G", picargs));
        startupScript.addCommand(() -> format("rename 's/(.+)_(.+)_(.+)_(.+)_(.+)__(.+)\\.fastq/$1_$2_$3_$4_R$6_$5.fastq/' %s/*.fastq", VmDirectories.OUTPUT));
        startupScript.addCommand(() -> format("pigz %s/*.fastq", VmDirectories.OUTPUT));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "bam2fastq"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("bam2fastq").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("Bam2Fastq", "Convert BAMs back to FASTQs",
                OperationDescriptor.InputType.FLAT);
    }
}
