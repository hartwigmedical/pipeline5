package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FILENAME;
import static com.hartwig.batch.api.RemoteLocationsApi.CRAM_FULL_PATH;
import static com.hartwig.batch.api.RemoteLocationsApi.getCramFileData;
import static com.hartwig.batch.operations.BatchCommon.BATCH_BENCHMARKS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.GNOMAD_DIR;
import static com.hartwig.batch.operations.BatchCommon.PAVE_DIR;
import static com.hartwig.batch.operations.BatchCommon.PAVE_JAR;
import static com.hartwig.batch.operations.BatchCommon.SAGE_DIR;
import static com.hartwig.batch.operations.BatchCommon.SAGE_JAR;
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

public class SageBenchmarks implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String[] inputData = descriptor.inputValue().split(",", -1);
        final String sampleId = inputData[0];
        final String referenceId = inputData[1];

        String runData = inputData[2];
        boolean runTumorNormal = runData.equals("TumorNormal");
        boolean runTumorOnly = runData.equals("TumorOnly");
        boolean runGermline = runData.equals("Germline");

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, SAGE_DIR, SAGE_JAR, VmDirectories.TOOLS));

        String tumorBamFile = String.format("%s.bam", sampleId);
        String referenceBamFile = String.format("%s.bam", referenceId);

        if(inputData.length >= 5)
        {
            String tumorBamDir = inputData[3];
            String refBamDir = inputData[4];

            if(runTumorNormal || runTumorOnly)
            {
                startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                        tumorBamDir, tumorBamFile, VmDirectories.INPUT));
            }

            if(runTumorNormal || runGermline)
            {
                startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                        refBamDir, referenceBamFile, VmDirectories.INPUT));
            }
        }
        else
        {
            // download tumor and ref BAMs as required
            if(runTumorNormal || runTumorOnly)
            {
                startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s* %s",
                        BATCH_BENCHMARKS_BUCKET, sampleId, tumorBamFile, VmDirectories.INPUT));
            }

            if(runTumorNormal || runGermline)
            {
                startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s* %s",
                        BATCH_BENCHMARKS_BUCKET, sampleId, referenceBamFile, VmDirectories.INPUT));
            }
        }

        RefGenomeVersion refGenomeVersion = inputData.length >= 6 ? RefGenomeVersion.valueOf(inputData[5]) : RefGenomeVersion.V37;
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(refGenomeVersion);

        // run Sage
        final StringJoiner sageArgs = new StringJoiner(" ");

        if(runTumorNormal || runTumorOnly)
        {
            sageArgs.add(String.format("-tumor %s", sampleId));
            sageArgs.add(String.format("-tumor_bam %s/%s", VmDirectories.INPUT, tumorBamFile));
        }
        else if(runGermline)
        {
            sageArgs.add(String.format("-tumor %s", referenceId));
            sageArgs.add(String.format("-tumor_bam %s/%s", VmDirectories.INPUT, referenceBamFile));
        }

        if(runTumorNormal)
        {
            sageArgs.add(String.format("-reference %s", referenceId));
            sageArgs.add(String.format("-reference_bam %s/%s", VmDirectories.INPUT, referenceBamFile));
        }
        else if(runGermline)
        {
            sageArgs.add(String.format("-reference %s", sampleId));
            sageArgs.add(String.format("-reference_bam %s/%s", VmDirectories.INPUT, tumorBamFile));
        }

        if(runGermline)
        {
            sageArgs.add(String.format("-hotspots %s", resourceFiles.sageGermlineHotspots()));
            sageArgs.add(String.format("-panel_bed %s", resourceFiles.sageGermlineCodingPanel()));
        }
        else
        {
            sageArgs.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
            sageArgs.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));
        }

        sageArgs.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));

        sageArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        sageArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        sageArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));

        if(runGermline)
        {
            sageArgs.add("-panel_only");
            sageArgs.add("-hotspot_min_tumor_qual 50");
            sageArgs.add("-panel_min_tumor_qual 75");
            sageArgs.add("-hotspot_max_germline_vaf 100");
            sageArgs.add("-hotspot_max_germline_rel_raw_base_qual 100");
            sageArgs.add("-panel_max_germline_vaf 100");
            sageArgs.add("-panel_max_germline_rel_raw_base_qual 100");
            sageArgs.add("-mnv_filter_enabled false");
        }

        String sageVcf;

        if(runTumorOnly)
            sageVcf = String.format("%s/%s.sage.tumor_only.vcf.gz", VmDirectories.OUTPUT, sampleId);
        else if(runGermline)
            sageVcf = String.format("%s/%s.sage.germline.vcf.gz", VmDirectories.OUTPUT, sampleId);
        else
            sageVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

        sageArgs.add(String.format("-out %s", sageVcf));
        sageArgs.add("-write_bqr_data");
        sageArgs.add(String.format("-perf_warn_time 50"));
        sageArgs.add(String.format("-threads %s", Bash.allCpus()));

        startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, sageArgs.toString()));

        // annotate with Pave - PON and gene impacts
        if(runTumorNormal || runTumorOnly)
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                    BATCH_TOOLS_BUCKET, PAVE_DIR, PAVE_JAR, VmDirectories.TOOLS));

            String ponFile = refGenomeVersion == RefGenomeVersion.V37 ?
                    "SageGermlinePon.1000x.37.tsv.gz" : "SageGermlinePon.98x.38.tsv.gz";

            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                    BATCH_RESOURCE_BUCKET, SAGE_DIR, ponFile, VmDirectories.INPUT));

            if(runTumorOnly && refGenomeVersion == RefGenomeVersion.V38)
            {
                startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/38/* %s",
                        BATCH_RESOURCE_BUCKET, GNOMAD_DIR, VmDirectories.INPUT));
            }

            final StringJoiner paveArgs = new StringJoiner(" ");

            String ponFilters = refGenomeVersion == RefGenomeVersion.V37 ?
                    "HOTSPOT:10:5;PANEL:6:5;UNKNOWN:6:0" : "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

            paveArgs.add(String.format("-sample %s", sampleId));
            paveArgs.add(String.format("-vcf_file %s", sageVcf)); // ponFilterVcf from BCF Tools

            paveArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
            paveArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
            paveArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
            paveArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
            paveArgs.add(String.format("-pon_file %s/%s", VmDirectories.INPUT, ponFile));
            paveArgs.add(String.format("-pon_filters \"%s\"", ponFilters));

            if(runTumorOnly && refGenomeVersion == RefGenomeVersion.V38)
            {
                paveArgs.add(String.format("-gnomad_freq_dir %s", VmDirectories.INPUT));
                paveArgs.add("-gnomad_load_chr_on_demand");
            }

            paveArgs.add("-read_pass_only");

            if(runTumorOnly)
                paveArgs.add("-write_pass_only");

            paveArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

            // final String paveVcf = String.format("%s/%s.sage.somatic.pon.pave.vcf.gz", VmDirectories.OUTPUT, sampleId);
            // paveArgs.add(String.format("-output_vcf_file %s", paveVcf));

            String paveJar = String.format("%s/%s", VmDirectories.TOOLS, PAVE_JAR);

            startupScript.addCommand(() -> format("java -jar %s %s", paveJar, paveArgs.toString()));
        }

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
        return OperationDescriptor.of("SageBenchmarks", "Sage benchmark samples", OperationDescriptor.InputType.FLAT);
    }
}
