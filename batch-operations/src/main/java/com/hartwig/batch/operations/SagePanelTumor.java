package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_RESOURCE_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.GNOMAD_DIR;
import static com.hartwig.batch.operations.BatchCommon.PANEL_BAM_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.PANEL_BED;
import static com.hartwig.batch.operations.BatchCommon.PAVE_DIR;
import static com.hartwig.batch.operations.BatchCommon.PAVE_JAR;
import static com.hartwig.batch.operations.BatchCommon.SAGE_DIR;
import static com.hartwig.batch.operations.BatchCommon.SAGE_JAR;
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

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V38);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, SAGE_DIR, SAGE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_RESOURCE_BUCKET, SAGE_DIR, PANEL_BED, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, PAVE_DIR, PAVE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/38/* %s",
                BATCH_RESOURCE_BUCKET, GNOMAD_DIR, VmDirectories.INPUT));

        String ponFile = "SageGermlinePon.98x.38.tsv.gz";
        String ponArtefactFile = "pon_panel_artefact.38.tsv";

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_RESOURCE_BUCKET, SAGE_DIR, ponFile, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_RESOURCE_BUCKET, SAGE_DIR, ponArtefactFile, VmDirectories.INPUT));

        // download tumor BAM
        final String tumorBam = String.format("%s.non_umi_dedup.bam", sampleId);

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s* %s",
                PANEL_BAM_BUCKET, tumorBam, VmDirectories.INPUT));

        final String sageVcf = String.format("%s/%s.sage.somatic.vcf.gz", VmDirectories.OUTPUT, sampleId);

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
        sageArgs.add(String.format("-out %s", sageVcf));

        sageArgs.add(String.format("-hotspot_min_tumor_qual 100"));
        sageArgs.add(String.format("-panel_min_tumor_qual 200"));
        sageArgs.add(String.format("-high_confidence_min_tumor_qual 200"));
        sageArgs.add(String.format("-low_confidence_min_tumor_qual 300"));

        sageArgs.add(String.format("-mnv_filter_enabled false"));
        sageArgs.add(String.format("-perf_warn_time 50"));
        // sageArgs.add(String.format("-log_debug"));
        sageArgs.add(String.format("-threads %s", Bash.allCpus()));

        startupScript.addCommand(() -> format("java -Xmx48G -jar %s/%s %s", VmDirectories.TOOLS, SAGE_JAR, sageArgs.toString()));

        // annotate with Pave - PON, Gnomad and gene impacts

        /*
        String bcfTools = String.format("%s/bcftools/%s/bcftools", VmDirectories.TOOLS, Versions.BCF_TOOLS);
        String ponVcf = String.format("%s/%s.sage.somatic.pon.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // /data/tools/bcftools/1.9/bcftools annotate -a /data/resources/bucket/sage/37/SageGermlinePon.1000x.37.vcf.gz
        // -c PON_COUNT,PON_MAX
        // FR16648814.sage.somatic.vcf.gz
        // -O z
        // -o FR16648814.sage.somatic.annotated.vcf.gz

        final StringJoiner ponArgs = new StringJoiner(" ");
        ponArgs.add(String.format("-a %s", resourceFiles.sageGermlinePon()));
        ponArgs.add("-c PON_COUNT,PON_MAX");
        ponArgs.add(String.format("%s", sageVcf));
        ponArgs.add("-O z");
        ponArgs.add(String.format("-o %s", ponVcf));

        startupScript.addCommand(() -> format("%s annotate %s", bcfTools, ponArgs.toString()));

        ///data/tools/bcftools/1.9/bcftools filter
        // -e ‘PON_COUNT!=“.” && INFO/TIER=“HOTSPOT” && PON_MAX>=5 && PON_COUNT >= 5’
        // -s PON -m+ FR16648814.sage.somatic.annotated.vcf.gz -O u
        // | /data/tools/bcftools/1.9/bcftools filter -e ‘PON_COUNT!=“.” && INFO/TIER=“PANEL” && PON_MAX>=5 && PON_COUNT >= 2’
        // -s PON -m+ -O u | /data/tools/bcftools/1.9/bcftools filter -e ‘PON_COUNT!=“.” && INFO/TIER!=“HOTSPOT” && INFO/TIER!=“PANEL” && PON_COUNT >= 2’
        // -s PON -m+ -O z -o FR16648814.sage.somatic.pon_filtered.vcf.gz

        String ponFilterVcf = String.format("%s/%s.sage.somatic.pon_filter.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // private static final String HOTSPOT = "INFO/TIER=\"HOTSPOT\" && PON_MAX>=%s && PON_COUNT >= %s";
        // private static final String PANEL = "INFO/TIER=\"PANEL\" && PON_MAX>=%s && PON_COUNT >= %s";
        // private static final String OTHER = "INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= %s";

        final StringJoiner ponFilterArgs = new StringJoiner(" ");
        ponFilterArgs.add("-e 'PON_COUNT!=\".\" && INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 5'");
        ponFilterArgs.add(String.format("-s PON -m+ %s -O u", ponVcf));
        ponFilterArgs.add(String.format("| %s filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 2'", bcfTools));
        ponFilterArgs.add(String.format("-s PON -m+ -O u | %s filter -e 'PON_COUNT!=\".\" && INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 2'", bcfTools));
        ponFilterArgs.add(String.format("-s PON -m+ -O z -o %s", ponFilterVcf));

        startupScript.addCommand(() -> format("%s filter %s", bcfTools, ponFilterArgs.toString()));
        */

        // finally run Pave
        final StringJoiner paveArgs = new StringJoiner(" ");
        String ponFilters = "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

        final String paveVcf = String.format("%s/%s.sage.somatic.pon.pave_pass.vcf.gz", VmDirectories.OUTPUT, sampleId);

        paveArgs.add(String.format("-sample %s", sampleId));
        paveArgs.add(String.format("-vcf_file %s", sageVcf)); // ponFilterVcf from BCF Tools

        paveArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        paveArgs.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        paveArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        paveArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        paveArgs.add("-only_canonical");
        paveArgs.add("-filter_pass");
        paveArgs.add(String.format("-gnomad_freq_dir %s", VmDirectories.INPUT));
        paveArgs.add(String.format("-pon_file %s/%s", VmDirectories.INPUT, ponFile));
        paveArgs.add(String.format("-pon_artefact_file %s/%s", VmDirectories.INPUT, ponArtefactFile));
        paveArgs.add(String.format("-pon_filters \"%s\"", ponFilters));

        paveArgs.add("-gnomad_load_chr_on_demand");
        paveArgs.add(String.format("-output_vcf_file %s", paveVcf));

        String paveJar = String.format("%s/%s", VmDirectories.TOOLS, PAVE_JAR);
        // String paveJar = String.format("%s/pave/%s/pave.jar", VmDirectories.TOOLS, Versions.PAVE);

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
        return OperationDescriptor.of("SagePanelTumor", "Sage Panel Tumor-Only", OperationDescriptor.InputType.FLAT);
    }
}
