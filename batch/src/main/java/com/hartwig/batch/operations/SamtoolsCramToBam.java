package com.hartwig.batch.operations;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import java.io.File;

public class SamtoolsCramToBam implements BatchOperation {
    public VirtualMachineJobDefinition execute(final InputFileDescriptor input, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
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
                        "/opt/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta",
                        "-o",
                        outputFile,
                        "-O",
                        "CRAM",
                        "-@",
                        Bash.allCpus(),
                        localInput));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "samtools"), executionFlags));
        return VirtualMachineJobDefinition.batchSamtoolsCram(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("SamtoolsCramToBam", "Produce a CRAM file from each input BAM");
    }
}
