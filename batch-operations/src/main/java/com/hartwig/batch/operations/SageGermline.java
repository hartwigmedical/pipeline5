package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.ApiInputFileDescriptorFactory;
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

public class SageGermline implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final InputFileDescriptor biopsy = inputs.get("biopsy");
        final ApiInputFileDescriptorFactory inputFileFactory = new ApiInputFileDescriptorFactory(biopsy);
        final String tumorSampleName = inputFileFactory.getTumor();
        final String referenceSampleName = inputFileFactory.getReference();
        final InputFileDescriptor tumorAlignment = inputFileFactory.getTumorAlignment();
        final InputFileDescriptor tumorAlignmentIndex = inputFileFactory.getTumorAlignmentIndex();
        final InputFileDescriptor referenceAlignment = inputFileFactory.getReferenceAlignment();
        final InputFileDescriptor referenceAlignmentIndex = inputFileFactory.getReferenceAlignmentIndex();

        final String localTumorFile = tumorAlignment.localDestination();
        final String localReferenceFile = referenceAlignment.localDestination();

        // Prepare SnpEff
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        commands.addCommand(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        // Download experimental JAR
//        commands.addCommand(downloadExperimentalVersion());

        // Download tumor
        commands.addCommand(() -> tumorAlignment.toCommandForm(localTumorFile));
        commands.addCommand(() -> tumorAlignmentIndex.toCommandForm(tumorAlignmentIndex.localDestination()));

        // Download normal
        commands.addCommand(() -> referenceAlignment.toCommandForm(localReferenceFile));
        commands.addCommand(() -> referenceAlignmentIndex.toCommandForm(referenceAlignmentIndex.localDestination()));

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).germlineMode(referenceSampleName, localReferenceFile, tumorSampleName, localTumorFile)
                        .addCoverage(resourceFiles.sageGermlineCoveragePanel());

        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SageGermlinePostProcess sagePostProcess = new SageGermlinePostProcess(referenceSampleName, tumorSampleName, resourceFiles);
        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addCommands(sageOutput.bash());

        // Store output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.sageGermlineCalling(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageGermline", "Generate sage output", OperationDescriptor.InputType.JSON);
    }

    private BashCommand downloadExperimentalVersion() {
        return () -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-germline/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar");
    }
}
