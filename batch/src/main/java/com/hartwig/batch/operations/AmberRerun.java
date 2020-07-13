package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.CopyLogToOutput;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberApplicationCommand;

public class AmberRerun implements BatchOperation {

    public static GoogleStorageLocation amberArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-amber", set, true);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("ref_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteReferenceFile = inputs.get("ref_cram");
        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index(".crai");
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index(".crai");

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        // Download tumor
        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));

        // Download normal
        commands.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        commands.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
        commands.addCommand(() -> new AmberApplicationCommand(referenceSampleName,
                localReferenceFile,
                tumorSampleName,
                localTumorFile,
                resourceFiles.refGenomeFile(),
                resourceFiles.amberHeterozygousLoci()).asBash());

        // Store output
        final GoogleStorageLocation archiveStorageLocation = amberArchiveDirectory(set);
        commands.addCommand(new CopyLogToOutput(executionFlags.log(), "run.log"));
        commands.addCommand(new OutputUpload(archiveStorageLocation));

        return VirtualMachineJobDefinition.amber(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("AmberRerun", "Generate amber output", OperationDescriptor.InputType.JSON);
    }

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }
}
