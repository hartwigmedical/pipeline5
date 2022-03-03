package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_BUCKET;
import static com.hartwig.batch.operations.BatchCommon.GRIPSS_DIR;
import static com.hartwig.batch.operations.BatchCommon.GRIPSS_JAR;
import static com.hartwig.batch.operations.BatchCommon.LINX_DIR;
import static com.hartwig.batch.operations.BatchCommon.LINX_JAR;
import static com.hartwig.batch.operations.BatchCommon.PURPLE_DIR;
import static com.hartwig.batch.operations.BatchCommon.PURPLE_JAR;
import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.utils.SampleLocationData;
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

public class GripssPurpleLinx implements BatchOperation {

    private static String COMBINED_OUTPUT_DIR = "gs://hmf-sv-analysis/gpl_batch";

    private static String PAVE_JAR = "/pave/1.0/pave.jar";

    // private static String PON_BP = "gridss_pon_breakpoint.37.sorted.bedpe";
    // private static String PON_BE = "gridss_pon_single_breakend.37.sorted.bed";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String[] inputArguments = descriptor.inputValue().split(",");

        final List<String> sampleIds = Arrays.stream(inputArguments[0].split(";")).collect(Collectors.toList());

        Map<String, SampleLocationData> sampleLocations = null;

        if(inputArguments.length > 1)
        {
            sampleLocations = SampleLocationData.loadSampleLocations(inputArguments[1], sampleIds);
        }
        else
        {
            sampleLocations = Maps.newHashMap();
        }

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // download required JARs and resources
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, GRIPSS_DIR, GRIPSS_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, PURPLE_DIR, PURPLE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_BUCKET, LINX_DIR, LINX_JAR, VmDirectories.TOOLS));

        // Gripss inputs
        // startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s", RESOURCE_DIR, GRIPSS_DIR, PON_BP, VmDirectories.INPUT));
        // startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s", RESOURCE_DIR, GRIPSS_DIR, PON_BE, VmDirectories.INPUT));

        for(String sampleId : sampleIds)
        {
            runSample(startupScript, resourceFiles, sampleId, sampleLocations);
        }

        // upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gpl"), executionFlags));

        // and copy the key output files to a single directory for convenience
        String gripssCombined = String.format("%s/gripss/", COMBINED_OUTPUT_DIR);
        String linxCombined = String.format("%s/linx/", COMBINED_OUTPUT_DIR);
        String purpleCombined = String.format("%s/purple/", COMBINED_OUTPUT_DIR);
        String paveCombined = String.format("%s/pave/", COMBINED_OUTPUT_DIR);

        startupScript.addCommand(() -> format("gsutil -m cp %s/*gripss*vcf* %s", VmDirectories.OUTPUT, gripssCombined));

        startupScript.addCommand(() -> format("gsutil -m cp %s/*sage.somatic.filtered.pave.vcf.gz* %s", VmDirectories.OUTPUT, paveCombined));

        // select files for subsequent Linx runs and/or comparison using Compar
        startupScript.addCommand(() -> format("gsutil -m cp %s/*linx*.tsv %s", VmDirectories.OUTPUT, linxCombined));

        startupScript.addCommand(() -> format("gsutil -m cp %s/*purple* %s", VmDirectories.OUTPUT, purpleCombined));
        startupScript.addCommand(() -> format("gsutil -m cp %s/*driver.catalog* %s", VmDirectories.OUTPUT, purpleCombined));
        // startupScript.addCommand(() -> format("gsutil -m cp %s/*purple*.tsv %s", VmDirectories.OUTPUT, purpleCombined));
        // startupScript.addCommand(() -> format("gsutil -m cp %s/*purple.qc %s", VmDirectories.OUTPUT, purpleCombined));
        // startupScript.addCommand(() -> format("gsutil -m cp %s/*purple*.vcf.gz* %s", VmDirectories.OUTPUT, purpleCombined));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("gpl")
                .startupCommand(startupScript)
                .performanceProfile(custom(8, 32))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    private void runSample(
            final BashStartupScript startupScript, final ResourceFiles resourceFiles,
            final String sampleId, final Map<String,SampleLocationData> sampleLocationsMap)
    {
        final SampleLocationData sampleLocations = sampleLocationsMap.containsKey(sampleId) ?
                sampleLocationsMap.get(sampleId)
                : SampleLocationData.fromRemoteLocationsApi(sampleId, new RemoteLocationsApi("hmf-crunch", sampleId));

        // download required input files
        String gridssVcf = sampleLocations.localFileRef(sampleLocations.GridssVcf);
        startupScript.addCommand(() -> sampleLocations.formDownloadRequest(sampleLocations.GridssVcf, false));

        // run Gripss
        final StringJoiner gripssArgs = new StringJoiner(" ");
        gripssArgs.add(String.format("-sample %s", sampleId));
        gripssArgs.add(String.format("-reference %s", sampleLocations.ReferenceId));
        gripssArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        gripssArgs.add(String.format("-known_hotspot_file %s", resourceFiles.knownFusionPairBedpe()));
        gripssArgs.add(String.format("-pon_sgl_file %s", resourceFiles.gridssBreakendPon())); // VmDirectories.INPUT, PON_BE
        gripssArgs.add(String.format("-pon_sv_file %s", resourceFiles.gridssBreakpointPon()));
        gripssArgs.add(String.format("-vcf %s", gridssVcf));
        gripssArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        gripssArgs.add(String.format("-log_debug"));

        String gripssJar = String.format("%s/%s", VmDirectories.TOOLS, GRIPSS_JAR);
        // String gripssJar = String.format("%s/gripss/%s/gripss.jar", VmDirectories.TOOLS, Versions.GRIPSS);

        startupScript.addCommand(() -> format("java -Xmx30G -jar %s %s", gripssJar, gripssArgs.toString()));

        final String gripssUnfilteredVcf = String.format("%s/%s.gripss.vcf.gz", VmDirectories.OUTPUT, sampleId);
        final String gripssFilteredVcf = String.format("%s/%s.gripss.filtered.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // Pave
        final StringJoiner paveArgs = new StringJoiner(" ");

        final String paveVcf = String.format("%s/%s.sage.somatic.filtered.pave.vcf.gz", VmDirectories.OUTPUT, sampleId);

        String sageSomaticVcf = sampleLocations.localFileRef(sampleLocations.SageVcf);
        startupScript.addCommand(() -> sampleLocations.formDownloadRequest(sampleLocations.SageVcf, false));

        paveArgs.add(String.format("-sample %s", sampleId));
        paveArgs.add(String.format("-vcf_file %s", sageSomaticVcf));

        paveArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        paveArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        paveArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        paveArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        paveArgs.add(String.format("-output_vcf_file %s", paveVcf));

        // String paveJar = String.format("%s/%s", VmDirectories.TOOLS, PAVE_JAR);
        String paveJar = String.format("%s/pave/%s/pave.jar", VmDirectories.TOOLS, Versions.PAVE);

        startupScript.addCommand(() -> format("java -jar %s %s", paveJar, paveArgs.toString()));

        // Purple
        // String amberDir = sampleLocations.localFileRef(sampleLocations.Amber);
        // startupScript.addCommand(() -> sampleLocations.formDownloadRequest(sampleLocations.Amber, true));
        String amberDir = VmDirectories.INPUT;
        String amberFiles = String.format("%s/*amber*", sampleLocations.Amber);
        startupScript.addCommand(() -> sampleLocations.formDownloadRequest(amberFiles, false));

        // String cobaltDir = sampleLocations.localFileRef(sampleLocations.Cobalt);
        // startupScript.addCommand(() -> sampleLocations.formDownloadRequest(sampleLocations.Cobalt, true));
        String cobaltDir = VmDirectories.INPUT;
        String cobaltFiles = String.format("%s/*cobalt*", sampleLocations.Cobalt);
        startupScript.addCommand(() -> sampleLocations.formDownloadRequest(cobaltFiles, false));

        final StringJoiner purpleArgs = new StringJoiner(" ");
        purpleArgs.add(String.format("-tumor %s", sampleId));
        purpleArgs.add(String.format("-reference %s", sampleLocations.ReferenceId));
        purpleArgs.add(String.format("-structural_vcf %s", gripssFilteredVcf));
        purpleArgs.add(String.format("-sv_recovery_vcf %s", gripssUnfilteredVcf));
        purpleArgs.add(String.format("-somatic_vcf %s", paveVcf));
        purpleArgs.add(String.format("-amber %s", amberDir));
        purpleArgs.add(String.format("-cobalt %s", cobaltDir));
        purpleArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        purpleArgs.add(String.format("-gc_profile %s", resourceFiles.gcProfileFile()));
        purpleArgs.add(String.format("-somatic_hotspots %s", resourceFiles.sageSomaticHotspots()));
        purpleArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        purpleArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        purpleArgs.add(String.format("-run_drivers"));
        purpleArgs.add(String.format("-no_charts"));
        purpleArgs.add(String.format("-threads %s", Bash.allCpus()));
        purpleArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        // String purpleJar = String.format("%s/%s", VmDirectories.TOOLS, PURPLE_JAR);
        String purpleJar = String.format("%s/purple/%s/purple.jar", VmDirectories.TOOLS, Versions.PURPLE);

        startupScript.addCommand(() -> format("java -jar %s %s", purpleJar, purpleArgs.toString()));

        final String purpleSvVcf = String.format("%s/%s.purple.sv.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // Linx
        final StringJoiner linxArgs = new StringJoiner(" ");
        linxArgs.add(String.format("-sample %s", sampleId));
        linxArgs.add(String.format("-sv_vcf %s", purpleSvVcf));
        linxArgs.add(String.format("-purple_dir %s", VmDirectories.OUTPUT));
        linxArgs.add(String.format("-fragile_site_file %s", resourceFiles.fragileSites()));
        linxArgs.add(String.format("-line_element_file %s", resourceFiles.lineElements()));
        linxArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        linxArgs.add(String.format("-check_drivers"));
        linxArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        linxArgs.add(String.format("-check_fusions"));
        linxArgs.add(String.format("-known_fusion_file %s", resourceFiles.knownFusionData()));
        linxArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        String linxJar = String.format("%s/%s", VmDirectories.TOOLS, LINX_JAR);
        // String linxJar = String.format("%s/linx/%s/linx.jar", VmDirectories.TOOLS, Versions.LINX);

        startupScript.addCommand(() -> format("java -jar %s %s", linxJar, linxArgs.toString()));
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GripssPurpleLinx", "Gripss-Purple-Linx rerun", OperationDescriptor.InputType.FLAT);
    }
}
