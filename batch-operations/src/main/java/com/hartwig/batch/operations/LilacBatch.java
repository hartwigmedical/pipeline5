package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.MAX_EXPECTED_BAM_SIZE_GB;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.api.RemoteLocationsDecorator;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class LilacBatch implements BatchOperation {

    private static final String LILAC_BATCH_BUCKET = "batch-lilac";
    private static final String LILAC_RESOURCES = String.format("%s/%s", LILAC_BATCH_BUCKET, "resources");
    private static final String LILAC_JAR = "lilac.jar";
    private static final String MAX_HEAP = "15G";

    private static final String LOCAL_LILAC_RESOURCES = String.format("%s/%s/", VmDirectories.RESOURCES, "lilac");

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // local controls
        int maxDistanceFromTop = 5;

        // Inputs: SampleId,ExpectedAlleles
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");
        final String sampleId = batchItems[0];
        final String expectedAlleles = batchItems.length >= 2 ? batchItems[1] : "";

        // Note: Can only use the bam slice decorator on biopsies that we ran LilacBamSlice on.
        final RemoteLocationsApi locationsApi = new RemoteLocationsApi(runData.billedProject(), sampleId);
        final LocalLocations localInput = new LocalLocations(new BamSliceDecorator(locationsApi));
        final String somaticVcf = localInput.getSomaticVariantsPurple();
        final String geneCopyNumber = localInput.getGeneCopyNumberTsv();
        final String tumorAlignment = localInput.getTumorAlignment();
        final String referenceAlignment = localInput.getReferenceAlignment();

        // download pilot Lilac jar
        commands.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s/%s %s",
                LILAC_RESOURCES, LILAC_JAR, VmDirectories.TOOLS));

        // create local resource directory and download resources
        commands.addCommand(createResourcesDir());

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s/hla_ref_* %s",
                LILAC_RESOURCES, LOCAL_LILAC_RESOURCES));

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s/lilac_* %s",
                LILAC_RESOURCES, LOCAL_LILAC_RESOURCES));

        // download sample input files
        commands.addCommands(localInput.generateDownloadCommands());

        // build Lilac arguments
        StringBuilder lilacArgs = new StringBuilder();

        /*

        boolean tumorSource = false;
    {
        bash = new JavaClassCommand("lilac",
                Versions.LILAC, JAR, MAIN_CLASS, MAX_HEAP,
                "-ref_genome", resourceFiles.refGenomeFile(),
                "-sample", sampleName,
                "-reference_bam", referenceBamPath, "-tumor_bam", tumorBamPath,
                "-output_dir", VmDirectories.OUTPUT, "-resource_dir", resourceFiles.lilacHlaSequences(),
                "-gene_copy_number", purpleGeneCopyNumberPath, "-somatic_vcf", purpleSomaticVariants,
                "-max_distance_from_top_score 10", "-threads", Bash.allCpus()).asBash();
    }
         */
        lilacArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));

        lilacArgs.append(String.format(" -sample %s", sampleId));
        lilacArgs.append(String.format(" -reference_bam %s", referenceAlignment));
        lilacArgs.append(String.format(" -tumor_bam %s", tumorAlignment));

        lilacArgs.append(String.format(" -output_dir %s/", VmDirectories.OUTPUT));

        lilacArgs.append(String.format(" -resource_dir %s", LOCAL_LILAC_RESOURCES));

        lilacArgs.append(String.format(" -gene_copy_number %s", geneCopyNumber));
        lilacArgs.append(String.format(" -somatic_vcf %s", somaticVcf));

        lilacArgs.append(String.format(" -threads %s", Bash.allCpus()));
        lilacArgs.append(String.format(" -max_distance_from_top_score %d", maxDistanceFromTop));

        if(!expectedAlleles.isEmpty())
        {
            lilacArgs.append(String.format(" -expected_alleles \"%s\"", expectedAlleles));
        }

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, lilacArgs.toString()));

        /*
        if(tumorSource)
        {
            commands.addCommand(new LilacApplicationCommand(resourceFiles, sampleId, tumorAlignment));
        }
         */

        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "lilac"), executionFlags));

        // in addition to standard batch bucket output, copy results to sampleId-named directories
        String runDirectory = "run_test_ref_02";

        String sampleOutputDir = String.format("gs://%s/%s/%s/", LILAC_BATCH_BUCKET, runDirectory, sampleId);
        commands.addCommand(() -> format("gsutil -m cp %s/* %s", VmDirectories.OUTPUT, sampleOutputDir));

        // and copy the run log files to a single directory for convenience
        String commonLogDir = String.format("gs://%s/%s/logs/", LILAC_BATCH_BUCKET, runDirectory);
        commands.addCommand(() -> format("gsutil -m cp /data/output/*.log %s", commonLogDir));
        commands.addCommand(() -> format("gsutil -m cp /data/output/*lilac*.txt %s", commonLogDir));

        // return VirtualMachineJobDefinition.lilac(commands, ResultsDirectory.defaultDirectory());

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(commands)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                // .workingDiskSpaceGb(MAX_EXPECTED_BAM_SIZE_GB)
                // .performanceProfile(VirtualMachinePerformanceProfile.custom(DEFAULT_CORES, maxRam))
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacBatch", "Generate lilac output", OperationDescriptor.InputType.FLAT);
    }

    private BashCommand createResourcesDir() {
        return () -> format("mkdir -p %s", LOCAL_LILAC_RESOURCES);
    }

    private BashCommand downloadNucleotideFiles(ResourceFiles resourceFiles) {
        return () -> format("gsutil -u hmf-crunch cp gs://%s/*_nuc.txt %s", LILAC_RESOURCES, LOCAL_LILAC_RESOURCES);
    }

    private BashCommand downloadProteinFiles(ResourceFiles resourceFiles) {
        return () -> format("gsutil -u hmf-crunch cp gs://%s/*_prot.txt %s", LILAC_RESOURCES, LOCAL_LILAC_RESOURCES);
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
