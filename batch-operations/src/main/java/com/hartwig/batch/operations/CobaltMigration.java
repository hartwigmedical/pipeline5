package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
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
import com.hartwig.pipeline.tertiary.cobalt.CobaltMigrationCommand;

public class CobaltMigration implements BatchOperation {

    public static GoogleStorageLocation cobaltArchiveDirectoryInput(final String set) {
        return GoogleStorageLocation.of("hmf-cobalt", set, true);
    }

    public static GoogleStorageLocation cobaltArchiveDirectoryOutput(final String set) {
        return GoogleStorageLocation.of("hmf-cobalt-1-10", set, true);
    }

    @Override

    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("ref_sample").inputValue();
        final GoogleStorageLocation remoteInputDirectory = cobaltArchiveDirectoryInput(set);

        // Download old files
        commands.addCommand(() -> copyInputCommand(remoteInputDirectory));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        commands.addCommand(() -> new CobaltMigrationCommand(resourceFiles, referenceSampleName, tumorSampleName).asBash());

        // Store output
        final GoogleStorageLocation archiveStorageLocation = cobaltArchiveDirectoryOutput(set);
        commands.addCommand(new CopyLogToOutput(executionFlags.log(), "run.log"));
        commands.addCommand(new OutputUpload(archiveStorageLocation));

        return VirtualMachineJobDefinition.cobalt(commands, ResultsDirectory.defaultDirectory());
    }

    private static String copyInputCommand(final GoogleStorageLocation targetLocation) {
        return format("gsutil -qm rsync -r gs://%s/%s %s/", targetLocation.bucket(), targetLocation.path(), VmDirectories.INPUT);
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("CobaltMigration", "Migrate COBALT to latest version", OperationDescriptor.InputType.JSON);
    }

}
