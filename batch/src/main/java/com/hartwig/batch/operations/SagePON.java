package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.somatic.SageCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SagePON implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        final String output = String.format("%s/SAGE.pon.vcf.gz", VmDirectories.OUTPUT);

        final BashCommand sageCommand = new SageCommand("com.hartwig.hmftools.sage.pon.PonApplication",
                "100G",
                "-in",
                VmDirectories.INPUT,
                "-out",
                output,
                "-threads",
                Bash.allCpus());

        // Download required resources
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-validation/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar"));

        // Download germline VCFS (and indexes)
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch -m cp %s %s",
                "gs://batch-sage-validation/first200/*.sage.somatic.vcf.gz",
                VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch -m cp %s %s",
                "gs://batch-sage-validation/first200/*.sage.somatic.vcf.gz.tbi",
                VmDirectories.INPUT));

        // Download germline VCFS (and indexes)
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch -m cp %s %s",
                "gs://batch-sage-validation/*/sage/*.sage.somatic.vcf.gz",
                VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch -m cp %s %s",
                "gs://batch-sage-validation/*/sage/*.sage.somatic.vcf.gz.tbi",
                VmDirectories.INPUT));

        // Run Pon Generator
        startupScript.addCommand(sageCommand);

        // Store output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));
        return VirtualMachineJobDefinition.sageCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    String getInput(List<InputFileDescriptor> inputs, String key) {
        return inputs.stream().filter(input -> input.name().equals(key)).collect(Collectors.toList()).get(0).inputValue();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SagePON", "Generate sage PON", OperationDescriptor.InputType.JSON);
    }
}
