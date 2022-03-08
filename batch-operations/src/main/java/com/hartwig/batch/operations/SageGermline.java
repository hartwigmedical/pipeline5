package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FILENAME;
import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FULL_PATH;
import static com.hartwig.batch.api.RemoteLocationsApi.getCramFileData;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.SageRerunOld.cramToBam;
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
import com.hartwig.pipeline.tools.Versions;

public class SageGermline implements BatchOperation {

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

        final RemoteLocationsApi locations = new RemoteLocationsApi("hmf-crunch", sampleId);

        String[] tumorCramData = getCramFileData(locations.getTumorAlignment());
        String tumorCramFile = tumorCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", tumorCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        String referenceId = locations.getReference();

        String[] refCramData = getCramFileData(locations.getReferenceAlignment());
        String refCramFile = refCramData[CRAM_FILENAME];

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s* %s", refCramData[CRAM_FULL_PATH], VmDirectories.INPUT));

        // download tumor CRAM
        String localTumorCram = String.format("%s/%s", VmDirectories.INPUT, tumorCramFile);
        String localRefCram = String.format("%s/%s", VmDirectories.INPUT, refCramFile);

        final String sageVcf = String.format("%s/%s.sage.germline.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner sageArgs = new StringJoiner(" ");

        // not the switch on samples
        sageArgs.add(String.format("-tumor %s", referenceId));
        sageArgs.add(String.format("-tumor_bam %s", localRefCram));
        sageArgs.add(String.format("-reference %s", sampleId));
        sageArgs.add(String.format("-reference_bam %s", localTumorCram));

        sageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
        sageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        sageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        sageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));

        sageArgs.add(String.format("-hotspots %s", resourceFiles.sageGermlineHotspots()));
        sageArgs.add(String.format("-panel_bed %s", resourceFiles.sageGermlineCodingPanel()));

        sageArgs.add("-panel_only");
        sageArgs.add("-hotspot_min_tumor_qual 50");
        sageArgs.add("-panel_min_tumor_qual 75");
        sageArgs.add("-hotspot_max_germline_vaf 100");
        sageArgs.add("-hotspot_max_germline_rel_raw_base_qual 100");
        sageArgs.add("-panel_max_germline_vaf 100");
        sageArgs.add("-panel_max_germline_rel_raw_base_qual 100");
        sageArgs.add("-mnv_filter_enabled false");

        sageArgs.add(String.format("-out %s", sageVcf));
        sageArgs.add(String.format("-threads %s", Bash.allCpus()));

        startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, sageArgs.toString()));

        // Pave germline
        String paveJar = String.format("%s/pave/%s/pave.jar", VmDirectories.TOOLS, Versions.PAVE);

        final String paveGermlineVcf = String.format("%s/%s.sage.germline.pave.vcf.gz", VmDirectories.OUTPUT, sampleId);

        StringJoiner paveGermlineArgs = new StringJoiner(" ");
        paveGermlineArgs.add(String.format("-sample %s", sampleId));
        paveGermlineArgs.add(String.format("-vcf_file %s", sageVcf));
        paveGermlineArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        paveGermlineArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        paveGermlineArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        paveGermlineArgs.add("-filter_pass");
        paveGermlineArgs.add(String.format("-output_vcf_file %s", paveGermlineVcf));

        startupScript.addCommand(() -> format("java -jar %s %s", paveJar, paveGermlineArgs.toString()));

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
        return OperationDescriptor.of("SageGermline", "Sage germline and Pave", OperationDescriptor.InputType.FLAT);
    }
}
