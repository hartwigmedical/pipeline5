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

import static java.lang.String.format;

public class SambambaCramaBam implements BatchOperation {
    public VirtualMachineJobDefinition execute(final InputFileDescriptor input, final RuntimeBucket bucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        String outputFile = VmDirectories.outputFile(new File(input.remoteFilename()).getName().replaceAll("\\.bam$", ".cram"));
        String localInput = String.format("%s/%s", VmDirectories.INPUT, new File(input.remoteFilename()).getName());
        startupScript.addCommand(() -> format("gsutil cp %s %s", input, localInput));
        startupScript.addCommand(new VersionedToolCommand("sambamba",
                "sambamba",
                Versions.SAMBAMBA,
                "view",
                localInput,
                "-o",
                outputFile,
                "-t",
                Bash.allCpus(),
                "--format=cram",
                "-T",
                "/opt/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta"));
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "cram"), executionFlags));
        return VirtualMachineJobDefinition.batchSambambaCram(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public CommandDescriptor descriptor() {
        return CommandDescriptor.of("SambambaCramaBam", "Produce a CRAM file from each input BAM");
    }
}
