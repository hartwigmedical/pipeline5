package com.hartwig.batch;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class CramConverter {
    private final RuntimeBucket bucket;
    private final ComputeEngine compute;

    public CramConverter(RuntimeBucket bucket, ComputeEngine compute) {
        this.bucket = bucket;
        this.compute = compute;
    }

    public PipelineStatus convert(String inputBam) {
        String outputFile = VmDirectories.outputFile(new File(inputBam).getName().replaceAll("\\.bam$", ".cram"));
        String localInput = format("%s/%s", VmDirectories.INPUT, new File(inputBam).getName());
        BashStartupScript startupScript = BashStartupScript.of(bucket.name());
        startupScript.addCommand(() -> format("gsutil cp %s %s", inputBam, localInput));
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
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "cram")));
        return compute.submit(bucket, VirtualMachineJobDefinition.cramMigration(startupScript, ResultsDirectory.defaultDirectory()));
    }
}
