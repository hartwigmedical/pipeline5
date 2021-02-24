package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.Collections;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.ApiInputFileDescriptorFactory;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssHardFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class GripssRerun implements BatchOperation {

    public static GoogleStorageLocation gripssSomaticFilteredFile(final String set, final String sample) {
        return GoogleStorageLocation.of("hmf-gripss", set + "/" + sample + ".gridss.somatic.filtered.vcf.gz", false);
    }

    public static GoogleStorageLocation gripssRecoveryFile(final String set, final String sample) {
        return GoogleStorageLocation.of("hmf-gripss", set + "/" + sample + ".gridss.somatic.vcf.gz", false);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        final InputFileDescriptor biopsy = inputs.get("biopsy");
        final ApiInputFileDescriptorFactory inputFileFactory = new ApiInputFileDescriptorFactory(biopsy);
        final String sample = inputFileFactory.getTumor();
        final String refSample = inputFileFactory.getReference();
        final InputFileDescriptor inputVcf = inputFileFactory.getGridssUnfilteredOutput();
        final InputFileDescriptor inputVcfIndex = inputVcf.index();

        final GridssSomaticFilter somaticFilter = new GridssSomaticFilter(resourceFiles, sample, refSample, inputVcf.localDestination());
        final SubStageInputOutput postProcessing = somaticFilter.andThen(new GridssHardFilter())
                .apply(SubStageInputOutput.of(sample, inputFile(inputVcf.localDestination()), Collections.emptyList()));

        // 0. Download latest jar file
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-gripss/resources/gripss.jar",
                "/opt/tools/gripss/" + Versions.GRIPSS + "/gripss.jar"));

        // 1. Download input files
        startupScript.addCommand(inputVcf::copyToLocalDestinationCommand);
        startupScript.addCommand(inputVcfIndex::copyToLocalDestinationCommand);

        //2. Run GRIPSS
        startupScript.addCommands(postProcessing.bash());

        // 4. Upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gripss"), executionFlags));
        return VirtualMachineJobDefinition.structuralPostProcessCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GripssRerun", "Run GRIPSS", OperationDescriptor.InputType.JSON);
    }

    private OutputFile inputFile(final String file) {
        return new OutputFile() {
            @Override
            public String fileName() {
                return file;
            }

            @Override
            public String path() {
                return file;
            }
        };
    }
}
