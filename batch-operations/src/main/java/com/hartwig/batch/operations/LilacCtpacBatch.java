package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.LILAC_DIR;
import static com.hartwig.batch.operations.BatchCommon.LILAC_JAR;
import static com.hartwig.batch.operations.LilacBatch.LILAC_BATCH_BUCKET;
import static com.hartwig.batch.operations.LilacBatch.addLilacDownloadCommands;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
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
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class LilacCtpacBatch implements BatchOperation {

    private static final String MAX_HEAP = "15G";
    private static final String PCAWG_BAM_BUCKET = "pcawg-hla-bams";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");

        String sampleId = batchItems[0];

        String runDirectory = "run_cptac_02";

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
        final String referenceBam = String.format("%s/%s.bam", VmDirectories.INPUT, sampleId);

        // download sample BAM files
        commands.addCommand(() -> format("gsutil -m -u hmf-crunch cp gs://%s/%s/* %s", PCAWG_BAM_BUCKET, sampleId, VmDirectories.INPUT));

        // build Lilac arguments

        String sampleOutputDir = String.format("%s/%s/", VmDirectories.OUTPUT, sampleId);
        commands.addCommand(() -> format("mkdir -p %s", sampleOutputDir));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V38);

        /*
        -sample C3N-01023_B -ref_genome /Users/charlesshale/data/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna
        -ref_genome_version V38
        -reference_bam /Users/charlesshale/data/lilac/pcawg/samples/C3N-01023_B/C3N-01023_B.bam
        -resource_dir /Users/charlesshale/data/lilac/ref/
        -output_dir /Users/charlesshale/data/lilac/pcawg/samples/C3N-01023_B/
         */

        StringBuilder lilacArgs = new StringBuilder();
        lilacArgs.append(String.format(" -sample %s", sampleId));
        lilacArgs.append(String.format(" -resource_dir %s", VmDirectories.INPUT));
        lilacArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
        lilacArgs.append(String.format(" -ref_genome_version %s", "V38"));
        lilacArgs.append(String.format(" -reference_bam %s", referenceBam));
        lilacArgs.append(String.format(" -output_dir %s", sampleOutputDir));
        lilacArgs.append(String.format(" -threads %s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, lilacArgs.toString()));

        String sampleRemoteOutputDir = String.format("gs://%s/%s/", LILAC_BATCH_BUCKET, runDirectory);
        commands.addCommand(() -> format("gsutil -m cp -r %s/%s/ %s", VmDirectories.OUTPUT, sampleId, sampleRemoteOutputDir));
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacCtpacBatch", "Generate lilac output", OperationDescriptor.InputType.FLAT);
    }
}
