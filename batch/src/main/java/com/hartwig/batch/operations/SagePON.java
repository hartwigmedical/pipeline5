package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SagePON implements BatchOperation {
    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
                                               final BashStartupScript startupScript, final RuntimeFiles executionFlags) {
        final String output = String.format("%s/SAGE.pon.vcf.gz", VmDirectories.OUTPUT);

        final BashCommand sageCommand = new JavaClassCommand("sage",
                "pilot",
                "sage.jar",
                "com.hartwig.hmftools.sage.pon.PonApplication",
                "16G",
                "-in",
                VmDirectories.INPUT,
                "-out",
                output);

        // Download required resources
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/resources/sage.jar", "/opt/tools/sage/pilot/sage.jar"));

        // Download germline VCFS (and indexes)
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/*/sage/*.sage.germline.vcf.gz", VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-sage/*/sage/*.sage.germline.vcf.gz.tbi", VmDirectories.INPUT));

        // Run Pon Generator
        startupScript.addCommand(sageCommand);

        // Store output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.builder().name("sage").startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 24))
                .namespacedResults(ResultsDirectory.defaultDirectory()).build();
    }

    String getInput(List<InputFileDescriptor> inputs, String key) {
        return inputs.stream().filter(input -> input.name().equals(key)).collect(Collectors.toList()).get(0).value();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SagePON", "Generate sage PON",
                OperationDescriptor.InputType.JSON);
    }
}
