package com.hartwig.batch.operations;

import static com.hartwig.batch.operations.GripssRerun.gripssRecoveryFile;
import static com.hartwig.batch.operations.GripssRerun.gripssSomaticFilteredFile;
import static com.hartwig.batch.operations.SageRerun.sageSomaticFilteredFile;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.CopyLogToOutput;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.ImmutableGoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleApplicationCommand;
import com.hartwig.pipeline.tools.Versions;

public class PurpleRerunTumorOnly implements BatchOperation {

    public static GoogleStorageLocation purpleArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-purple-tumor-only", set, true);
    }


    public List<BashCommand> bashCommands(final InputBundle inputs) {
        final List<BashCommand> commands = Lists.newArrayList();
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();

        final GoogleStorageLocation sageVcfStorage = sageSomaticFilteredFile(set, tumorSampleName);
        final GoogleStorageLocation sageIndexStorage = index(sageVcfStorage);

        final GoogleStorageLocation gripssVcfStorage = gripssSomaticFilteredFile(set, tumorSampleName);
        final GoogleStorageLocation gripssVcfIndexStorage = index(gripssVcfStorage);

        final GoogleStorageLocation gripssRecoveryVcfStorage = gripssRecoveryFile(set, tumorSampleName);
        final GoogleStorageLocation gripssRecoveryVcfIndexStorage = index(gripssRecoveryVcfStorage);
        final String amberInputDir = VmDirectories.INPUT + "/amber";
        final String cobaltInputDir = VmDirectories.INPUT + "/cobalt";

        final InputDownload amberLocation = new InputDownload(AmberRerunTumorOnly.amberArchiveDirectory(set), amberInputDir);
        final InputDownload cobaltLocation = new InputDownload(CobaltTumorOnlyRerun.cobaltArchiveDirectory(set), cobaltInputDir);
        final InputDownload sageLocation = new InputDownload(sageVcfStorage);
        final InputDownload sageLocationIndex = new InputDownload(sageIndexStorage);

        final InputDownload gripssLocation = new InputDownload(gripssVcfStorage);
        final InputDownload gripssLocationIndex = new InputDownload(gripssVcfIndexStorage);

        final InputDownload gripssRecoveryLocation = new InputDownload(gripssRecoveryVcfStorage);
        final InputDownload gripssRecoveryLocationIndex = new InputDownload(gripssRecoveryVcfIndexStorage);

        commands.add(() -> "mkdir -p " + amberInputDir);
        commands.add(() -> "mkdir -p " + cobaltInputDir);
        commands.add(amberLocation);
        commands.add(cobaltLocation);
        commands.add(sageLocation);
        commands.add(sageLocationIndex);
        commands.add(gripssLocation);
        commands.add(gripssLocationIndex);
        commands.add(gripssRecoveryLocation);
        commands.add(gripssRecoveryLocationIndex);

        BashCommand purpleCommand = new PurpleApplicationCommand(resourceFiles,
                tumorSampleName,
                amberLocation.getLocalTargetPath(),
                cobaltLocation.getLocalTargetPath(),
                sageLocation.getLocalTargetPath(),
                gripssLocation.getLocalTargetPath(),
                gripssRecoveryLocation.getLocalTargetPath(),
                VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos",
                false);

        commands.add(purpleCommand);
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {
        final String set = inputs.get("set").inputValue();
        final GoogleStorageLocation archiveStorageLocation = purpleArchiveDirectory(set);

        commands.addCommands(bashCommands(inputs));
        commands.addCommand(new CopyLogToOutput(executionFlags.log(), "run.log"));
        commands.addCommand(new OutputUpload(archiveStorageLocation));

        return VirtualMachineJobDefinition.purple(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("PurpleRerunTumorOnly", "Generate PURPLE output", OperationDescriptor.InputType.JSON);
    }

    private static GoogleStorageLocation index(GoogleStorageLocation victim) {
        if (victim.isDirectory()) {
            throw new IllegalArgumentException();
        }
        return ImmutableGoogleStorageLocation.builder().from(victim).path(victim.path() + ".tbi").build();
    }
}
