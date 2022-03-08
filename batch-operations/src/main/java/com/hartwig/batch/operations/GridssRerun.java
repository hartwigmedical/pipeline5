package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.GridssBackport.index;
import static com.hartwig.batch.operations.GridssBackport.remoteUnfilteredVcfArchivePath;
import static com.hartwig.batch.operations.SageRerunOld.cramToBam;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GridssRerun implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("reference_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteReferenceFile = inputs.get("ref_cram");

        final InputFileDescriptor runData = inputs.get();
        final RemoteLocationsApi locationsApi = new RemoteLocationsApi(runData.billedProject(), tumorSampleName);

        InputDownload tumorBamDownload = new InputDownload(locationsApi.getTumorAlignment());
        InputDownload tumorBamIndexDownload = new InputDownload(locationsApi.getTumorAlignmentIndex());
        InputDownload referenceBamDownload = new InputDownload(locationsApi.getReferenceAlignment());
        InputDownload referenceBamIndexDownload = new InputDownload(locationsApi.getReferenceAlignmentIndex());

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index();
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index();

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        final String tumorBamPath = localTumorFile.replace("cram", "bam");
        final String refBamPath = localReferenceFile.replace("cram", "bam");

        Driver driver = new Driver(resourceFiles,
                referenceSampleName,
                tumorSampleName,
                VmDirectories.outputFile(tumorSampleName + ".assembly.bam"),
                refBamPath,
                tumorBamPath);
        GridssAnnotation viralAnnotation = new GridssAnnotation(resourceFiles, false);
        SubStageInputOutput unfilteredVcfOutput = driver.andThen(viralAnnotation).apply(SubStageInputOutput.empty(tumorSampleName));

        final OutputFile unfilteredVcf = unfilteredVcfOutput.outputFile();
        final OutputFile unfilteredVcfIndex = unfilteredVcf.index(".tbi");
        final GoogleStorageLocation unfilteredVcfRemoteLocation = remoteUnfilteredVcfArchivePath(set, tumorSampleName);
        final GoogleStorageLocation unfilteredVcfIndexRemoteLocation = index(unfilteredVcfRemoteLocation, ".tbi");

        // COMMANDS
        commands.addCommand(new ExportPathCommand(new BwaCommand()));
        commands.addCommand(new ExportPathCommand(new SamtoolsCommand()));
        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));
        commands.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        commands.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));
        if (!localTumorFile.equals(tumorBamPath)) {
            commands.addCommands(cramToBam(localTumorFile));
        }
        if (!localReferenceFile.equals(refBamPath)) {
            commands.addCommands(cramToBam(localReferenceFile));
        }
        commands.addCommands(unfilteredVcfOutput.bash());
        commands.addCommand(() -> unfilteredVcf.copyToRemoteLocation(unfilteredVcfRemoteLocation));
        commands.addCommand(() -> unfilteredVcfIndex.copyToRemoteLocation(unfilteredVcfIndexRemoteLocation));
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gridss"), executionFlags));

        return VirtualMachineJobDefinition.structuralCalling(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GridssRerun", "Generate gridss unfiltered output", OperationDescriptor.InputType.JSON);
    }

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }
}
