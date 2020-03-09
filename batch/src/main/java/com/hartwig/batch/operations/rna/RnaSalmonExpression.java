package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class RnaSalmonExpression implements BatchOperation {

    private static final String SALMON_TRANSCRIPTOME = "/opt/resources/salmon_transcriptome";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {
        InputFileDescriptor descriptor = inputs.get();

        // final String sampleId = descriptor.value();

        // TMP: enable salmon for execution
        // startupScript.addCommand(() -> "chmod a+x /opt/tools/salmon/1.0.0/salmon");

        // copy down FASTQ files for this sample
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://rna-fastqs/*/*.gz", VmDirectories.INPUT));

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("$(ls %s/*_R1* | tr '\\n' ',')", VmDirectories.INPUT);
        final String r2Files = format("$(ls %s/*_R2* | tr '\\n' ',')", VmDirectories.INPUT);

        // logging
        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run the STAR mapper
        final String[] salmonArgs = {"quant", "-i", SALMON_TRANSCRIPTOME, "-l", "A", "-1", r1Files, "-2", r2Files,
                "-p", threadCount, "-o", VmDirectories.OUTPUT};

        startupScript.addCommand(new VersionedToolCommand("salmon", "salmon", "1.0.0", salmonArgs));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "salmon"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-salmon").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(650)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 36)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaSalmonExpression", "Run Salmon to generate transcript expression from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
