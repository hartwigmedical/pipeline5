package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.LILAC_JAR;
import static com.hartwig.batch.operations.LilacBatch.LILAC_BATCH_BUCKET;
import static com.hartwig.batch.operations.LilacBatch.addLilacDownloadCommands;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
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

public class LilacPcawgBatch implements BatchOperation {

    private static final String MAX_HEAP = "15G";
    private static final String PCAWG_BAM_BUCKET = "pcawg-hla-bams";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        // Inputs: SampleId,ExpectedAlleles
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");

        String sampleId = batchItems[0];

        String runDirectory = "run_pcawg_02";

        // download pilot Lilac jar
        addLilacDownloadCommands(commands);

        addSampleCommands(runData, commands, runDirectory, sampleId);

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

    private void addSampleCommands(
            final InputFileDescriptor runData, final BashStartupScript commands,
            final String runDirectory, final String sampleId)
    {
        final String referenceAlignment = String.format("%s/%s_ref.bam", VmDirectories.INPUT, sampleId);
        final String tumorAlignment = String.format("%s/%s_tumor.bam", VmDirectories.INPUT, sampleId);

        // download sample BAM files
        commands.addCommand(() -> format("gsutil -m -u hmf-crunch cp gs://%s/%s/* %s", PCAWG_BAM_BUCKET, sampleId, VmDirectories.INPUT));

        // build Lilac arguments

        String sampleOutputDir = String.format("%s/%s/", VmDirectories.OUTPUT, sampleId);
        commands.addCommand(() -> format("mkdir -p %s", sampleOutputDir));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        StringBuilder lilacArgs = new StringBuilder();
        lilacArgs.append(String.format(" -sample %s", sampleId));
        lilacArgs.append(String.format(" -resource_dir %s", VmDirectories.INPUT));
        lilacArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
        lilacArgs.append(String.format(" -reference_bam %s", referenceAlignment));
        lilacArgs.append(String.format(" -tumor_bam %s", tumorAlignment));
        lilacArgs.append(" -run_id REF");
        lilacArgs.append(String.format(" -output_dir %s", sampleOutputDir));
        // lilacArgs.append(String.format(" -gene_copy_number_file %s", geneCopyNumber));
        // lilacArgs.append(String.format(" -somatic_variants_file %s", somaticVcf));
        lilacArgs.append(" -max_elim_candidates 500");
        lilacArgs.append(String.format(" -threads %s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, lilacArgs.toString()));

        // and a tumor-only run
        String tumorOutputDir = String.format("%s/%s/tumor", VmDirectories.OUTPUT, sampleId);
        commands.addCommand(() -> format("mkdir -p %s", tumorOutputDir));

        StringBuilder tumorLilacArgs = new StringBuilder();
        tumorLilacArgs.append(String.format(" -sample %s", sampleId));
        tumorLilacArgs.append(String.format(" -resource_dir %s", VmDirectories.INPUT));
        tumorLilacArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
        tumorLilacArgs.append(String.format(" -reference_bam %s", tumorAlignment));
        tumorLilacArgs.append(" -run_id TUMOR");
        lilacArgs.append(" -max_elim_candidates 500");
        tumorLilacArgs.append(String.format(" -output_dir %s", tumorOutputDir));
        tumorLilacArgs.append(String.format(" -threads %s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, tumorLilacArgs.toString()));

        String sampleRemoteOutputDir = String.format("gs://%s/%s/", LILAC_BATCH_BUCKET, runDirectory);
        commands.addCommand(() -> format("gsutil -m cp -r %s/%s/ %s", VmDirectories.OUTPUT, sampleId, sampleRemoteOutputDir));
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacPcawgBatch", "Generate lilac output", OperationDescriptor.InputType.FLAT);
    }
}
