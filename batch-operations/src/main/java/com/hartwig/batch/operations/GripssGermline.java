package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.BatchCommon.BATCH_TOOLS_DIR;
import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import java.util.StringJoiner;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocationsApi;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
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

public class GripssGermline implements BatchOperation {

    private static String GRIPSS_DIR = "gripss";
    private static String GRIPSS_JAR = "gripss-kt.jar";
    private static final String MAX_HEAP = "30G";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.inputValue();

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);
        final LocalLocations inputFileFactory = new LocalLocations(new RemoteLocationsApi(descriptor.billedProject(), sampleId));
        final String referenceId = inputFileFactory.getReference();

        final String inputVcf = inputFileFactory.getStructuralVariantsGridss();

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                BATCH_TOOLS_DIR, GRIPSS_DIR, GRIPSS_JAR, VmDirectories.TOOLS));

        startupScript.addCommands(inputFileFactory.generateDownloadCommands());

        // run GRIPSS
        final String outputVcf1 = String.format("%s/%s.gripss.vcf.gz", VmDirectories.OUTPUT, referenceId);

        final StringJoiner gripssArgs = new StringJoiner(" ");
        gripssArgs.add(String.format("-tumor %s", referenceId));
        gripssArgs.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        gripssArgs.add(String.format("-breakpoint_hotspot %s", resourceFiles.knownFusionPairBedpe()));
        gripssArgs.add(String.format("-breakend_pon %s", resourceFiles.gridssBreakendPon()));
        gripssArgs.add(String.format("-breakpoint_pon %s", resourceFiles.gridssBreakpointPon()));
        gripssArgs.add(String.format("-pon_distance %d", 4));
        gripssArgs.add(String.format("-min_qual_break_end %d", 400));
        gripssArgs.add(String.format("-min_qual_rescue_mobile_element_insertion %d", 400));
        gripssArgs.add(String.format("-min_qual_break_point %d", 250));
        gripssArgs.add(String.format("-input_vcf %s", inputVcf));
        gripssArgs.add(String.format("-output_vcf %s", outputVcf1));

        startupScript.addCommand(() -> format("java -Xmx%s -cp %s/%s com.hartwig.hmftools.gripsskt.GripssApplicationKt %s",
                MAX_HEAP, VmDirectories.TOOLS, GRIPSS_JAR, gripssArgs.toString()));

        final String outputVcf2 = String.format("%s/%s.gripss.filtered.vcf.gz", VmDirectories.OUTPUT, referenceId);

        final StringJoiner gripss2Args = new StringJoiner(" ");
        gripss2Args.add(String.format("-input_vcf %s", outputVcf1));
        gripss2Args.add(String.format("-output_vcf %s", outputVcf2));

        startupScript.addCommand(() -> format("java -Xmx%s -cp %s/%s com.hartwig.hmftools.gripsskt.GripssHardFilterApplicationKt %s",
                MAX_HEAP, VmDirectories.TOOLS, GRIPSS_JAR, gripss2Args.toString()));

        // upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gripss"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("gripss")
                .startupCommand(startupScript)
                .performanceProfile(custom(8, 30))
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GripssGermline", "GRIPSS Germline", OperationDescriptor.InputType.FLAT);
    }
}
