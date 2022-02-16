package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
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
import com.hartwig.pipeline.tertiary.amber.AmberCommandBuilder;

public class AmberRerunTumorOnly implements BatchOperation {

    public static GoogleStorageLocation amberArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-amber-tumor-only", set, true);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index();

        final String localTumorFile = localFilename(remoteTumorFile);

        // Download tumor
        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        commands.addCommand(() -> AmberCommandBuilder.newBuilder(resourceFiles).tumor(tumorSampleName, localTumorFile).build().asBash());

        // Store output
        final GoogleStorageLocation archiveStorageLocation = amberArchiveDirectory(set);
        commands.addCommand(new CopyLogToOutput(executionFlags.log(), "run.log"));
        commands.addCommand(new OutputUpload(archiveStorageLocation));

        return VirtualMachineJobDefinition.amber(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("AmberRerunTumorOnly", "Generate amber output", OperationDescriptor.InputType.JSON);
    }

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }
}
