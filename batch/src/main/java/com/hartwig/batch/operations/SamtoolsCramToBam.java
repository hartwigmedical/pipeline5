package com.hartwig.batch.operations;

import com.hartwig.batch.BatchOperation;
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
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import java.io.File;
import java.util.List;

public class SamtoolsCramToBam implements BatchOperation {
    public VirtualMachineJobDefinition execute(final List<InputFileDescriptor> inputs, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        InputFileDescriptor input = inputs.get(0);
        String outputFile = VmDirectories.outputFile(new File(input.remoteFilename()).getName().replaceAll("\\.bam$", ".cram"));
        String localInput = String.format("%s/%s", VmDirectories.INPUT, new File(input.remoteFilename()).getName());
        startupScript.addCommand(() -> input.toCommandForm(localInput));
        startupScript.addCommand(() -> "tar -C /opt/tools/samtools/1.9 -xvf /opt/tools/samtools/1.9/samtools.tar.gz");
        startupScript.addCommand(
                new VersionedToolCommand("samtools",
                        "samtools",
                        Versions.SAMTOOLS,
                        "view",
                        "-C",
                        "-h",
                        "-T",
                        Resource.REFERENCE_GENOME_FASTA,
                        "-o",
                        outputFile,
                        "-O",
                        "CRAM",
                        "-@",
                        Bash.allCpus(),
                        localInput));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "samtools"), executionFlags));
        return VirtualMachineJobDefinition.builder().name("samtoolscram").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 4)).build();
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("SamtoolsCramToBam", "Produce a CRAM file from each inputs BAM");
    }
}
