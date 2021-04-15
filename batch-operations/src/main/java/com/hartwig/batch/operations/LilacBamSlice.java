package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class LilacBamSlice implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // Inputs
        final InputFileDescriptor biopsy = inputs.get("biopsy");
        final RemoteLocationsApi remoteLocationsApi = new RemoteLocationsApi(biopsy);
        final LocalLocations localInput = new LocalLocations(remoteLocationsApi);
        final String tumorAlignment = localInput.getTumorAlignment();
        final String tumorHlaAlignment = VmDirectories.OUTPUT + "/" + remoteLocationsApi.getTumor() + ".hla.bam";
        final String referenceAlignment = localInput.getReferenceAlignment();
        final String referenceHlaAlignment = VmDirectories.OUTPUT + "/" + remoteLocationsApi.getReference() + ".hla.bam";

        // Download Resources
        commands.addCommand(createResourcesDir());
        commands.addCommand(downloadBedFile(resourceFiles));

        // Download Crams
        commands.addCommands(localInput.generateDownloadCommands());

        // Slice and index
        commands.addCommand(SamtoolsCommand.sliceToUncompressedBam(resourceFiles, resourceFiles.lilacHlaSequences() + "/hla.bed", referenceAlignment, referenceHlaAlignment));
        commands.addCommand(SamtoolsCommand.index(referenceHlaAlignment));
        commands.addCommand(SamtoolsCommand.sliceToUncompressedBam(resourceFiles, resourceFiles.lilacHlaSequences() + "/hla.bed", tumorAlignment, tumorHlaAlignment));
        commands.addCommand(SamtoolsCommand.index(tumorHlaAlignment));

        // 4. Upload output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of("hla-bams", remoteLocationsApi.getTumor()), executionFlags));
        return VirtualMachineJobDefinition.lilacSlice(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacBamSlice", "Generate amber output", OperationDescriptor.InputType.JSON);
    }

    private BashCommand createResourcesDir() {
        return () -> "mkdir -p /opt/resources/lilac/";
    }

    private BashCommand downloadBedFile(ResourceFiles resourceFiles) {
        return () -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-lilac/resources/hla.bed", resourceFiles.lilacHlaSequences());
    }
}
