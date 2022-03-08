package com.hartwig.batch.operations;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.ImmutableGoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleCommandBuilder;

public class PurpleRerun implements BatchOperation {

    public List<BashCommand> bashCommands(final RemoteLocations locations) {

        final LocalLocations batchInput = new LocalLocations(locations);
        final List<BashCommand> commands = Lists.newArrayList();
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        final String tumorSampleName = batchInput.getTumor();
        final String referenceSampleName = batchInput.getReference();

        final String amberLocation = batchInput.getAmber();
        final String cobaltLocation = batchInput.getCobalt();
        final String sageSomaticLocation = batchInput.getSomaticVariantsSage();
        final String sageGermlineLocation = batchInput.getGermlineVariantsSage();
        final String gripssLocation = batchInput.getStructuralVariantsGripss();
        final String gripssRecoveryLocation = batchInput.getStructuralVariantsGripssRecovery();

        commands.addAll(batchInput.generateDownloadCommands());

        PurpleCommandBuilder builder = new PurpleCommandBuilder(resourceFiles,
                amberLocation,
                cobaltLocation,
                tumorSampleName,
                gripssLocation,
                gripssRecoveryLocation,
                sageSomaticLocation).addGermline(sageGermlineLocation);

        commands.add(builder.build());
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        final InputFileDescriptor biopsy = inputs.get("biopsy");
        final RemoteLocationsApi storageLocations = new RemoteLocationsApi(biopsy);

        commands.addCommands(bashCommands(storageLocations));
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "purple"), executionFlags));

        return VirtualMachineJobDefinition.purple(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("PurpleRerun", "Generate PURPLE output", OperationDescriptor.InputType.JSON);
    }

    private static GoogleStorageLocation index(final GoogleStorageLocation victim) {
        if (victim.isDirectory()) {
            throw new IllegalArgumentException();
        }
        return ImmutableGoogleStorageLocation.builder().from(victim).path(victim.path() + ".tbi").build();
    }
}
