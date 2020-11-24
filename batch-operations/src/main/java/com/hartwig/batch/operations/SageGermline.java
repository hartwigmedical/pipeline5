package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.somatic.SageApplication;
import com.hartwig.pipeline.calling.somatic.SageCommandBuilder;
import com.hartwig.pipeline.calling.somatic.SagePostProcessGermline;
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
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("ref_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteReferenceFile = inputs.get("ref_cram");

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index();
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index();

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        // Prepare SnpEff
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG19);
        commands.addCommand(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        // Download experimental JAR
        commands.addCommand(downloadExperimentalVersion());

        // Download tumor
        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));

        // Download normal
        commands.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        commands.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));

        final SageCommandBuilder sageCommandBuilder = new SageCommandBuilder(resourceFiles).germlineMode(referenceSampleName,
                localReferenceFile,
                tumorSampleName,
                localTumorFile);

        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SagePostProcessGermline sagePostProcess = new SagePostProcessGermline(referenceSampleName, tumorSampleName, resourceFiles);
        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addCommands(sageOutput.bash());

        // Store output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.sageCalling(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageGermline", "Generate sage output", OperationDescriptor.InputType.JSON);
    }

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }

    private BashCommand downloadExperimentalVersion() {
        return () -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-germline/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar");
    }
}
