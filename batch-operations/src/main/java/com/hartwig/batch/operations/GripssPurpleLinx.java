package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
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

public class GripssPurpleLinx implements BatchOperation {

    private static String RESOURCE_DIR = "gs://hmf-crunch-resources";
    private static String GRIPSS_DIR = "gripss";
    private static String GRIPSS_JAR = "gripss.jar";
    private static String GRIPSS_OLD_JAR = "gripss-kt.jar";

    private static String PURPLE_DIR = "purple";
    private static String PURPLE_JAR = "purple.jar";

    private static String LINX_DIR = "linx";
    private static String LINX_JAR = "linx.jar";

    private static String PON_BP = "gridss_pon_breakpoint.37.sorted.bedpe";
    private static String PON_BE = "gridss_pon_single_breakend.37.sorted.bed";

    private static final String MAX_HEAP = "30G";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        final LocalLocations inputFileFactory = new LocalLocations(new RemoteLocationsApi(descriptor.billedProject(), sampleId));
        final String referenceId = inputFileFactory.getReference();

        // JARs
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, GRIPSS_DIR, GRIPSS_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, GRIPSS_DIR, GRIPSS_OLD_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, PURPLE_DIR, PURPLE_JAR, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, LINX_DIR, LINX_JAR, VmDirectories.TOOLS));

        // Gripss inputs
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, GRIPSS_DIR, PON_BP, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RESOURCE_DIR, GRIPSS_DIR, PON_BE, VmDirectories.INPUT));

        final String gridssVcf = inputFileFactory.getStructuralVariantsGridss();

        // Purple inputs

        final String amberLocation = inputFileFactory.getAmber();
        final String cobaltLocation = inputFileFactory.getCobalt();
        final String sageSomaticLocation = inputFileFactory.getSomaticVariantsSage();

        // Linx inputs

        startupScript.addCommands(inputFileFactory.generateDownloadCommands());

        // run new Gripss
        final StringJoiner gripssArgs = new StringJoiner(" ");
        gripssArgs.add(String.format("-sample %s", sampleId));
        gripssArgs.add(String.format("-reference %s", referenceId));
        gripssArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        gripssArgs.add(String.format("-known_hotspot_file %s", resourceFiles.knownFusionPairBedpe()));
        gripssArgs.add(String.format("-pon_sgl_file %s/%s", VmDirectories.INPUT, PON_BE));
        gripssArgs.add(String.format("-pon_sv_file %s/%s", VmDirectories.INPUT, PON_BP));
        gripssArgs.add(String.format("-vcf %s", gridssVcf));
        gripssArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        gripssArgs.add(String.format("-log_debug"));

        startupScript.addCommand(() -> format("java -jar %s/%s %s",
                VmDirectories.TOOLS, GRIPSS_JAR, gripssArgs.toString()));

        final String gripssUnfilteredVcf = String.format("%s/%s.gripss.vcf.gz", VmDirectories.OUTPUT, sampleId);
        final String gripssFilteredVcf = String.format("%s/%s.gripss.filtered.vcf.gz", VmDirectories.OUTPUT, sampleId);

        // run old Gripss
        final String oldGripssUnfilteredVcf = String.format("%s/%s.gripss.old.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner oldGripssArgs = new StringJoiner(" ");
        oldGripssArgs.add(String.format("-tumor %s", sampleId));
        oldGripssArgs.add(String.format("-reference %s", referenceId));
        oldGripssArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        oldGripssArgs.add(String.format("-breakpoint_hotspot %s", resourceFiles.knownFusionPairBedpe()));
        oldGripssArgs.add(String.format("-breakend_pon %s/%s", VmDirectories.INPUT, PON_BE));
        oldGripssArgs.add(String.format("-breakpoint_pon %s/%s", VmDirectories.INPUT, PON_BP));
        oldGripssArgs.add(String.format("-input_vcf %s", gridssVcf));
        oldGripssArgs.add(String.format("-output_vcf %s", oldGripssUnfilteredVcf));

        startupScript.addCommand(() -> format("java -Xmx%s -cp %s/%s com.hartwig.hmftools.gripsskt.GripssApplicationKt %s",
                MAX_HEAP, VmDirectories.TOOLS, GRIPSS_OLD_JAR, oldGripssArgs.toString()));

        final String oldGripssFilteredVcf = String.format("%s/%s.gripss.old.filtered.vcf.gz", VmDirectories.OUTPUT, sampleId);

        final StringJoiner oldGripssHfArgs = new StringJoiner(" ");
        oldGripssHfArgs.add(String.format("-input_vcf %s", oldGripssUnfilteredVcf));
        oldGripssHfArgs.add(String.format("-output_vcf %s", oldGripssFilteredVcf));

        startupScript.addCommand(() -> format("java -cp %s/%s com.hartwig.hmftools.gripsskt.GripssHardFilterApplicationKt %s",
                VmDirectories.TOOLS, GRIPSS_OLD_JAR, oldGripssHfArgs.toString()));

        // compare new vs old outputs
        final StringJoiner compareUnfilteredArgs = new StringJoiner(" ");
        compareUnfilteredArgs.add(String.format("-sample %s", sampleId));
        compareUnfilteredArgs.add(String.format("-original_vcf %s", oldGripssUnfilteredVcf));
        compareUnfilteredArgs.add(String.format("-new_vcf %s", gripssUnfilteredVcf));
        compareUnfilteredArgs.add(String.format("-output_id unfiltered"));
        compareUnfilteredArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        startupScript.addCommand(() -> format("java -cp %s/%s com.hartwig.hmftools.gripss.GripssCompareVcfs %s",
                VmDirectories.TOOLS, GRIPSS_JAR, compareUnfilteredArgs.toString()));

        // and again for filtered output
        final StringJoiner compareFilteredArgs = new StringJoiner(" ");
        compareFilteredArgs.add(String.format("-sample %s", sampleId));
        compareFilteredArgs.add(String.format("-original_vcf %s", oldGripssFilteredVcf));
        compareFilteredArgs.add(String.format("-new_vcf %s", gripssFilteredVcf));
        compareFilteredArgs.add(String.format("-output_id filtered"));
        compareFilteredArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        startupScript.addCommand(() -> format("java -cp %s/%s com.hartwig.hmftools.gripss.GripssCompareVcfs %s",
                VmDirectories.TOOLS, GRIPSS_JAR, compareFilteredArgs.toString()));

        // Purple
        final StringJoiner purpleArgs = new StringJoiner(" ");
        purpleArgs.add(String.format("-tumor %s", sampleId));
        purpleArgs.add(String.format("-reference %s", referenceId));
        purpleArgs.add(String.format("-structural_vcf %s", gripssFilteredVcf));
        purpleArgs.add(String.format("-sv_recovery_vcf %s", gripssUnfilteredVcf));
        purpleArgs.add(String.format("-somatic_vcf %s", sageSomaticLocation));
        purpleArgs.add(String.format("-amber %s", amberLocation));
        purpleArgs.add(String.format("-cobalt %s", cobaltLocation));
        purpleArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        purpleArgs.add(String.format("-gc_profile %s", resourceFiles.gcProfileFile()));
        purpleArgs.add(String.format("-somatic_hotspots %s", resourceFiles.sageSomaticHotspots()));
        purpleArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        purpleArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        purpleArgs.add(String.format("-run_drivers"));
        purpleArgs.add(String.format("-no_charts"));
        purpleArgs.add(String.format("-threads %s", Bash.allCpus()));
        purpleArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        startupScript.addCommand(() -> format("java -jar %s/%s %s",
                VmDirectories.TOOLS, PURPLE_JAR, purpleArgs.toString()));

        // Linx
        final StringJoiner linxArgs = new StringJoiner(" ");
        linxArgs.add(String.format("-sample %s", sampleId));
        linxArgs.add(String.format("-sv_vcf %s", gripssFilteredVcf));
        linxArgs.add(String.format("-purple_dir %s", VmDirectories.OUTPUT));
        linxArgs.add(String.format("-fragile_site_file %s", resourceFiles.fragileSites()));
        linxArgs.add(String.format("-line_element_file %s", resourceFiles.lineElements()));
        linxArgs.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        linxArgs.add(String.format("-check_drivers"));
        linxArgs.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        linxArgs.add(String.format("-check_fusions"));
        linxArgs.add(String.format("-known_fusion_file %s", resourceFiles.knownFusionData()));
        linxArgs.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        startupScript.addCommand(() -> format("java -jar %s/%s %s",
                VmDirectories.TOOLS, LINX_JAR, linxArgs.toString()));

        // upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gpl"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("gpl")
                .startupCommand(startupScript)
                .performanceProfile(custom(8, 30))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GripssPurpleLinx", "Gripss-Purple-Linx rerun", OperationDescriptor.InputType.FLAT);
    }
}
