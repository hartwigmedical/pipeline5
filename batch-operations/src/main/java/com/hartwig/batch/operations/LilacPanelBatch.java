package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.LILAC_DIR;
import static com.hartwig.batch.operations.BatchCommon.LILAC_JAR;
import static com.hartwig.batch.operations.BatchCommon.PANEL_BAM_BUCKET;
import static com.hartwig.batch.operations.LilacBatch.addLilacDownloadCommands;
import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.StringJoiner;

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

public class LilacPanelBatch implements BatchOperation {

    private static final String MAX_HEAP = "30G";

    private static final String LOCAL_LILAC_RESOURCES = String.format("%s/%s/", VmDirectories.RESOURCES, "lilac");

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        // Inputs: SampleId,ExpectedAlleles
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");

        String sampleId = batchItems[0];

        // download pilot Lilac jar
        addLilacDownloadCommands(commands);

        String tumorBam = String.format("%s.non_umi_dedup.bam", sampleId);

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                PANEL_BAM_BUCKET, tumorBam, VmDirectories.INPUT));

        // build Lilac arguments
        // String sampleOutputDir = String.format("%s/%s/", VmDirectories.OUTPUT, sampleId);
        // commands.addCommand(() -> format("mkdir -p %s", sampleOutputDir));
        // String runDirectory = "run_panel";

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V38);

        StringJoiner lilacArgs = new StringJoiner(" ");
        lilacArgs.add(String.format("-sample %s", sampleId));
        lilacArgs.add(String.format("-reference_bam %s/%s", VmDirectories.INPUT, tumorBam));
        lilacArgs.add(String.format("-resource_dir %s/", VmDirectories.INPUT));
        lilacArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        lilacArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        lilacArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        lilacArgs.add("-write_all_files");
        lilacArgs.add(String.format("-threads %s", Bash.allCpus()));

        String lilacJar = String.format("%s/%s", VmDirectories.TOOLS, LILAC_JAR);
        // String lilacJar = String.format("%s/lilac/%s/lilac.jar", VmDirectories.TOOLS, Versions.LILAC);

        commands.addCommand(() -> format("java -Xmx%s -jar %s %s", MAX_HEAP, lilacJar, lilacArgs.toString()));

        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "lilac"), executionFlags));

        // and copy the run log files to a single directory for convenience
        // String commonLogDir = String.format("gs://%s/%s/logs/", LILAC_BATCH_BUCKET, runDirectory);
        // commands.addCommand(() -> format("gsutil -m cp /data/output/*.log %s", commonLogDir));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(commands)
                .performanceProfile(custom(12, 32))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    /*
    private void addSampleCommands(
            final BashStartupScript commands, final String runDirectory, final String sampleId)
    {
        String tumorBam = String.format("%s.non_umi_dedup.bam", sampleId);

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                PANEL_BAM_BUCKET, tumorBam, VmDirectories.INPUT));

        // build Lilac arguments

        String sampleOutputDir = String.format("%s/%s/", VmDirectories.OUTPUT, sampleId);
        commands.addCommand(() -> format("mkdir -p %s", sampleOutputDir));

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        StringBuilder lilacArgs = new StringBuilder();
        lilacArgs.append(String.format(" -sample %s", sampleId));
        lilacArgs.append(String.format(" -resource_dir %s", VmDirectories.INPUT));
        lilacArgs.append(String.format(" -ref_genome %s", resourceFiles.refGenomeFile()));
        lilacArgs.append(String.format(" -ref_genome_version %s", resourceFiles.version().toString()));
        lilacArgs.append(String.format(" -reference_bam %s/%s", VmDirectories.INPUT, tumorBam));
        lilacArgs.append(String.format(" -output_dir %s", sampleOutputDir));
        lilacArgs.append(String.format(" -threads %s", Bash.allCpus()));

        commands.addCommand(() -> format("java -Xmx%s -jar %s/%s %s",
                MAX_HEAP, VmDirectories.TOOLS, LILAC_JAR, lilacArgs.toString()));

        // String sampleRemoteOutputDir = String.format("gs://%s/%s/", LILAC_BATCH_BUCKET, runDirectory);
        //commands.addCommand(() -> format("gsutil -m cp -r %s/%s/ %s", VmDirectories.OUTPUT, sampleId, sampleRemoteOutputDir));
    }
    */

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("LilacPanelBatch", "Run Lilac on panel tumor-only", OperationDescriptor.InputType.FLAT);
    }
}
