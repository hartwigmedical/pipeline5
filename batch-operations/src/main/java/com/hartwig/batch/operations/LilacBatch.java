package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.api.RemoteLocationsDecorator;
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
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.lilac.LilacApplicationCommand;
import com.hartwig.pipeline.tools.Versions;

public class LilacBatch implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // Inputs
        final InputFileDescriptor biopsy = inputs.get("biopsy");
//        final LocalLocations localInput = new LocalLocations((new RemoteLocationsApi(biopsy)));

        // Note: Can only use the bam slice decorator on biopsies that we ran LilacBamSlice on.
        final LocalLocations localInput = new LocalLocations(new BamSliceDecorator(new RemoteLocationsApi(biopsy)));
        final String somaticVcf = localInput.getSomaticVariantsPurple();
        final String geneCopyNumber = localInput.getGeneCopyNumberTsv();
        final String tumorSampleName = localInput.getTumor();
        final String tumorAlignment = localInput.getTumorAlignment();
        final String referenceAlignment = localInput.getReferenceAlignment();

        // Download Resources
        commands.addCommand(createResourcesDir());
        commands.addCommand(downloadExperimentalVersion());
        commands.addCommand(downloadNucleotideFiles(resourceFiles));
        commands.addCommand(downloadProteinFiles(resourceFiles));

        // Download Crams
        commands.addCommands(localInput.generateDownloadCommands());

        // Execute Lilac
        commands.addCommand(new LilacApplicationCommand(resourceFiles,
                tumorSampleName,
                referenceAlignment,
                tumorAlignment,
                geneCopyNumber,
                somaticVcf));


        // 4. Upload output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "lilac"), executionFlags));
        commands.addCommand(() -> "gsutil -m cp /data/output/*.log gs://batch-lilac3/output/");
        commands.addCommand(() -> "gsutil -m cp /data/output/*lilac*.txt gs://batch-lilac3/output/");
        return VirtualMachineJobDefinition.lilac(commands, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacBatch", "Generate lilac output", OperationDescriptor.InputType.JSON);
    }

    private BashCommand createResourcesDir() {
        return () -> "mkdir -p /opt/resources/lilac/";
    }

    private BashCommand downloadNucleotideFiles(ResourceFiles resourceFiles) {
        return () -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-lilac/resources/*_nuc.txt", resourceFiles.lilacHlaSequences());
    }

    private BashCommand downloadProteinFiles(ResourceFiles resourceFiles) {
        return () -> format("gsutil -u hmf-crunch cp %s %s", "gs://batch-lilac/resources/*_prot.txt", resourceFiles.lilacHlaSequences());
    }

    private BashCommand downloadExperimentalVersion() {
        return () -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-lilac/resources/lilac.jar",
                "/opt/tools/lilac/" + Versions.LILAC + "/lilac.jar");
    }

    class BamSliceDecorator extends RemoteLocationsDecorator {

        public BamSliceDecorator(final RemoteLocations decorator) {
            super(decorator);
        }

        @Override
        public GoogleStorageLocation getReferenceAlignment() {
            return GoogleStorageLocation.of("hla-bams", getTumor() + "/" + getReference() + ".hla.bam");
        }

        @Override
        public GoogleStorageLocation getReferenceAlignmentIndex() {
            return GoogleStorageLocation.of("hla-bams", getTumor() + "/" + getReference() + ".hla.bam.bai");
        }

        @Override
        public GoogleStorageLocation getTumorAlignment() {
            return GoogleStorageLocation.of("hla-bams", getTumor() + "/" + getTumor() + ".hla.bam");
        }

        @Override
        public GoogleStorageLocation getTumorAlignmentIndex() {
            return GoogleStorageLocation.of("hla-bams", getTumor() + "/" + getTumor() + ".hla.bam.bai");
        }
    }

}
