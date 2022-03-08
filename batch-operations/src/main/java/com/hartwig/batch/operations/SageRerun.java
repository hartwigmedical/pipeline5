package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FILENAME;
import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FULL_PATH;
import static com.hartwig.batch.api.RemoteLocationsApi.getCramFileData;
import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.PAVE_DIR;
import static com.hartwig.batch.operations.BatchCommon.PAVE_JAR;
import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
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

public class SageRerun implements BatchOperation {

    private static final String SAGE_DIR = "sage";
    private static final String SAGE_JAR = "sage.jar";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, SAGE_DIR, SAGE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, PAVE_DIR, PAVE_JAR, VmDirectories.TOOLS));

        String ponFile = "SageGermlinePon.1000x.37.tsv.gz";

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_RESOURCE_BUCKET, SAGE_DIR, ponFile, VmDirectories.INPUT));

        // download tumor and ref CRAM
        final RemoteLocationsApi locations = new RemoteLocationsApi("hmf-crunch", sampleId);

        String[] tumorCramData = getCramFileData(locations.getTumorAlignment());
        String tumorCramFile = tumorCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", tumorCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        String referenceId = locations.getReference();

        String[] refCramData = getCramFileData(locations.getReferenceAlignment());
        String refCramFile = refCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", refCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        final String sageVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // run Sage
        final StringJoiner sageArgs = new StringJoiner(" ");
        sageArgs.add(String.format("-tumor %s", sampleId));
        sageArgs.add(String.format("-tumor_bam %s/%s", VmDirectories.INPUT, tumorCramFile));
        sageArgs.add(String.format("-reference %s", referenceId));
        sageArgs.add(String.format("-reference_bam %s/%s", VmDirectories.INPUT, refCramFile));
        sageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
        sageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
        sageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));

        sageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        sageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        sageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        sageArgs.add(String.format("-out %s", sageVcf));

        sageArgs.add(String.format("-perf_warn_time 50"));
        // sageArgs.add(String.format("-log_debug"));
        sageArgs.add(String.format("-threads %s", Bash.allCpus()));

        startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, sageArgs.toString()));

        // annotate with Pave - PON and gene impacts
        final StringJoiner paveArgs = new StringJoiner(" ");
        String ponFilters = "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

        final String paveVcf = String.format("%s/%s.sage.somatic.pon.pave.vcf.gz", VmDirectories.OUTPUT, sampleId);

        paveArgs.add(String.format("-sample %s", sampleId));
        paveArgs.add(String.format("-vcf_file %s", sageVcf)); // ponFilterVcf from BCF Tools

        paveArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        paveArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        paveArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        paveArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        paveArgs.add(String.format("-pon_file %s/%s", VmDirectories.INPUT, ponFile));
        paveArgs.add(String.format("-pon_filters \"%s\"", ponFilters));
        paveArgs.add(String.format("-output_vcf_file %s", paveVcf));

        String paveJar = String.format("%s/%s", VmDirectories.TOOLS, PAVE_JAR);

        startupScript.addCommand(() -> format("java -jar %s %s", paveJar, paveArgs.toString()));

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
        return OperationDescriptor.of("SageRerun", "Sage + Pave Rerun", OperationDescriptor.InputType.FLAT);
    }
}
