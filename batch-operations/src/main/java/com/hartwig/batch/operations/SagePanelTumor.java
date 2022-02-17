package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_DIR;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_DIR;
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

public class SagePanelTumor implements BatchOperation {

    private static final String PANEL_BAM_BUCKET = "gs://hmf-crunch-experiments/211005_david_FUNC-89_panel-v2-coverage-analysis/bam/VALEXP07";
    private static final String SAGE_DIR = "sage";
    private static final String SAGE_JAR = "sage.jar";
    private static final String PANEL_BED = "primary_targets_restricted_transcripts.bed";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V38);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_DIR, SAGE_DIR, SAGE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_RESOURCE_DIR, SAGE_DIR, PANEL_BED, VmDirectories.INPUT));

        // download tumor BAM
        final String tumorBam = String.format("%s.non_umi_dedup.bam", sampleId);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                PANEL_BAM_BUCKET, tumorBam, VmDirectories.INPUT));

        final String outputVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // run Sage
        final StringJoiner sageArgs = new StringJoiner(" ");
        sageArgs.add(String.format("-tumor %s", sampleId));
        sageArgs.add(String.format("-tumor_bam %s/%s", VmDirectories.INPUT, tumorBam));
        sageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
        sageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
        sageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));

        sageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        sageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        sageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        sageArgs.add(String.format("-coverage_bed %s/%s", VmDirectories.INPUT, PANEL_BED));
        sageArgs.add(String.format("-out %s", outputVcf));

        // sageArgs.add(String.format("-max_read_depth 1000000"));
        // sageArgs.add(String.format("-max_read_depth_panel 1000000"));
        // sageArgs.add(String.format("-max_realignment_depth 1000000"));
        sageArgs.add(String.format("-mnv_filter_enabled false"));
        sageArgs.add(String.format("-perf_warn_time 50"));
        // sageArgs.add(String.format("-log_debug"));
        sageArgs.add(String.format("-threads %s", Bash.allCpus()));

        startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, sageArgs.toString()));

        // upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("sage")
                .startupCommand(startupScript)
                .performanceProfile(custom(24, 64))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SagePanelTumor", "Sage Panel Tumor-Only", OperationDescriptor.InputType.FLAT);
    }
}
