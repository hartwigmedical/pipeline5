package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageApplication;
import com.hartwig.pipeline.calling.sage.SageCommandBuilder;
import com.hartwig.pipeline.calling.sage.SageGermlinePostProcess;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageGermlineOld implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final InputFileDescriptor biopsy = inputs.get("biopsy");
        final LocalLocations localInput = new LocalLocations(new RemoteLocationsApi(biopsy));
        final String tumorSampleName = localInput.getTumor();
        final String referenceSampleName = localInput.getReference();
        final String tumorAlignment = localInput.getTumorAlignment();
        final String referenceAlignment = localInput.getReferenceAlignment();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // Download Inputs
        commands.addCommands(localInput.generateDownloadCommands());

        /*

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).germlineMode(referenceSampleName, referenceAlignment, tumorSampleName, tumorAlignment)
                        .addCoverage();

        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SageGermlinePostProcess sagePostProcess = new SageGermlinePostProcess(referenceSampleName, tumorSampleName, resourceFiles);
        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addCommands(sageOutput.bash());

        // Store output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));
         */

        return VirtualMachineJobDefinition.sageGermlineCalling(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageGermlineOld", "Generate sage output", OperationDescriptor.InputType.JSON);
    }

    private BashCommand downloadExperimentalVersion() {
        return () -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-germline/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar");
    }
}
