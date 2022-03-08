package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.LILAC_DIR;
import static com.hartwig.batch.operations.BatchCommon.LILAC_JAR;
import static com.hartwig.batch.operations.HlaBamSlicer.HLA_BAMS_BUCKET;

import java.util.List;
import java.util.StringJoiner;

import com.google.common.collect.Lists;
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
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class LilacBatch implements BatchOperation {

    public static final String LILAC_BATCH_BUCKET = "hmf-hla-lilac";
    private static final String MAX_HEAP = "15G";

    private static final String LOCAL_LILAC_RESOURCES = String.format("%s/%s/", VmDirectories.RESOURCES, "lilac");

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        // Inputs: SampleId,ExpectedAlleles
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");

        List<String> sampleIds = Lists.newArrayList(batchItems[0]);
        boolean hasRna = batchItems.length > 1 && batchItems[1].equals("RNA");

        // String runDirectory = "run_ref_18";
        String runDirectory = "run_ref_non_truth_01";

        // download pilot Lilac jar
        addLilacDownloadCommands(commands);

        for(String sampleId : sampleIds)
        {
            addSampleCommands(runData, commands, runDirectory, sampleId, hasRna);
        }

        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "lilac"), executionFlags));

        // and copy the run log files to a single directory for convenience
        String commonLogDir = String.format("gs://%s/%s/logs/", LILAC_BATCH_BUCKET, runDirectory);
        commands.addCommand(() -> format("gsutil -m cp /data/output/*.log %s", commonLogDir));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(commands)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    public static void addLilacDownloadCommands(final BashStartupScript commands)
    {
        // download pilot Lilac jar and resources
        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, LILAC_DIR, LILAC_JAR, VmDirectories.TOOLS));

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/hla_ref_* %s",
                BATCH_RESOURCE_BUCKET, LILAC_DIR, VmDirectories.INPUT));

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/lilac_* %s",
                BATCH_RESOURCE_BUCKET, LILAC_DIR, VmDirectories.INPUT));
    }

    private void addSampleCommands(
            final InputFileDescriptor runData, final BashStartupScript commands,
            final String runDirectory, final String sampleId, final boolean hasRna)
    {
        final RemoteLocationsApi locationsApi = new RemoteLocationsApi(runData.billedProject(), sampleId);
        final LocalLocations localInput = new LocalLocations(new BamSliceDecorator(locationsApi));
        final String somaticVcf = localInput.getSomaticVariantsPurple();
        final String geneCopyNumber = localInput.getGeneCopyNumberTsv();
        final String tumorAlignment = localInput.getTumorAlignment();
        final String referenceAlignment = localInput.getReferenceAlignment();
        final String rnaAlignment = hasRna ? String.format("%s.rna.hla.bam", sampleId) : "";

        // download sample input files
        commands.addCommands(localInput.generateDownloadCommands());

        if(hasRna)
        {
            commands.addCommand(() -> format("gsutil -m cp gs://%s/%s/%s* %s",
                    HLA_BAMS_BUCKET, sampleId, rnaAlignment, VmDirectories.INPUT));
        }

        // build Lilac arguments

        String sampleOutputDir = String.format("%s/%s/", VmDirectories.OUTPUT, sampleId);
        commands.addCommand(() -> format("mkdir -p %s", sampleOutputDir));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        StringJoiner lilacArgs = new StringJoiner(" ");
        lilacArgs.add(String.format(" -sample %s", sampleId));
        lilacArgs.add(String.format(" -resource_dir %s", VmDirectories.INPUT));
        lilacArgs.add(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
        lilacArgs.add(String.format(" -reference_bam %s", referenceAlignment));
        lilacArgs.add(String.format(" -tumor_bam %s", tumorAlignment));

        if(hasRna)
        {
            lilacArgs.add(String.format(" -rna_bam %s/%s", VmDirectories.INPUT, rnaAlignment));
        }

        lilacArgs.add(String.format(" -output_dir %s", sampleOutputDir));
        lilacArgs.add(String.format(" -gene_copy_number_file %s", geneCopyNumber));
        lilacArgs.add(String.format(" -somatic_variants_file %s", somaticVcf));
        lilacArgs.add(String.format(" -threads %s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, lilacArgs.toString()));

        /*
        if(tumorOnly)
        {
            String tumorOutputDir = String.format("%s/%s/tumor", VmDirectories.OUTPUT, sampleId);
            commands.addCommand(() -> format("mkdir -p %s", tumorOutputDir));

            StringBuilder tumorLilacArgs = new StringBuilder();
            tumorLilacArgs.add(String.format(" -sample %s", sampleId));
            tumorLilacArgs.add(String.format(" -resource_dir %s", LOCAL_LILAC_RESOURCES));
            tumorLilacArgs.add(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
            tumorLilacArgs.add(String.format(" -reference_bam %s", tumorAlignment));
            tumorLilacArgs.add(" -tumor_only");
            tumorLilacArgs.add(String.format(" -output_dir %s", tumorOutputDir));
            tumorLilacArgs.add(String.format(" -threads %s", Bash.allCpus()));

            commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                    MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, tumorLilacArgs.toString()));
        }
        */

        String sampleRemoteOutputDir = String.format("gs://%s/%s/", LILAC_BATCH_BUCKET, runDirectory);
        commands.addCommand(() -> format("gsutil -m cp -r %s/%s/ %s", VmDirectories.OUTPUT, sampleId, sampleRemoteOutputDir));
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacBatch", "Generate lilac output", OperationDescriptor.InputType.FLAT);
    }

    class BamSliceDecorator extends RemoteLocationsDecorator {

        public BamSliceDecorator(final RemoteLocations decorator) {
            super(decorator);
        }

        @Override
        public GoogleStorageLocation getReferenceAlignment() {
            return GoogleStorageLocation.of(HLA_BAMS_BUCKET, getTumor() + "/" + getReference() + ".hla.bam");
        }

        @Override
        public GoogleStorageLocation getReferenceAlignmentIndex() {
            return GoogleStorageLocation.of(HLA_BAMS_BUCKET, getTumor() + "/" + getReference() + ".hla.bam.bai");
        }

        @Override
        public GoogleStorageLocation getTumorAlignment() {
            return GoogleStorageLocation.of(HLA_BAMS_BUCKET, getTumor() + "/" + getTumor() + ".hla.bam");
        }

        @Override
        public GoogleStorageLocation getTumorAlignmentIndex() {
            return GoogleStorageLocation.of(HLA_BAMS_BUCKET, getTumor() + "/" + getTumor() + ".hla.bam.bai");
        }
    }
}
