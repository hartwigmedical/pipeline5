package com.hartwig.batch.operations;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.Hg19ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class BachelorCohort implements BatchOperation {

    private static final String BACHELOR_RESOURCES = "gs://bachelor-wide";

    private static final String CLINVAR_FILTERS = "wide_germline_carriership_clinvar_filters.csv";
    private static final String BACHELOR_XML_CONFIG = "wide_germline_carriership_program.xml";

    private static final String BACHELOR_JAR = "/opt/tools/bachelor/1.12/bachelor.jar";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.inputValue();

        // format: SetId,RefSampleId,TumorSampleId,RefCramFile,TumorCramFile,GermlineVcfFile
        final String[] batchItems = batchInputs.split(",");

        if(batchItems.length < 6)
        {
            System.out.print(String.format("invalid input arguments(%d) vs expected(6) data(%s)", batchItems.length, batchInputs));
            return null;
        }

        final String sampleId = batchItems[2];

        // eg gs://hmf-cram-150720-hmfregcpct-hmfxx5-hmfxx6-cpct02020171/CPCT02020171T_dedup.realigned.cram
        final String tumorBamPath = batchItems[4];
        final String[] tumorBamComponents = tumorBamPath.split("/");
        final String tumorBamFile = tumorBamComponents[tumorBamComponents.length - 1];
        final String tumorBamIndexPath = tumorBamPath + ".crai";

        // eg gs://hmf-output-2018-48/180731_HMFregCPCT_FR17019763_FR16983319_CPCT02030541/180731_HMFregCPCT_FR17019763_FR16983319_CPCT02030541.annotated.vcf.gz
        final String germlineVcfPath = batchItems[5];
        final String[] germlineVcfComponents = germlineVcfPath.split("/");
        final String germlineVcfFile = germlineVcfComponents[germlineVcfComponents.length - 1];

        // copy down BAM and VCF file for this sample
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", tumorBamIndexPath, VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", tumorBamPath, VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s* %s", germlineVcfPath, VmDirectories.INPUT));

        final Hg19ResourceFiles resourceFiles = new Hg19ResourceFiles();
        final String refGenome = resourceFiles.refGenomeFile();

        // copy down required reference files
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", BACHELOR_RESOURCES, CLINVAR_FILTERS, VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", BACHELOR_RESOURCES, BACHELOR_XML_CONFIG, VmDirectories.INPUT));

        // TEMP: copy down bachelor JAR
        // startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", BACHELOR_RESOURCES, "bachelor.jar", VmDirectories.TOOLS));
        // final String bachelorJar = String.format("%s/%s", VmDirectories.TOOLS, "bachelor.jar");
        final String bachelorJar = BACHELOR_JAR;

        // run Bachelor
        StringBuilder bachelorArgs = new StringBuilder();
        bachelorArgs.append(String.format("-sample %s", sampleId));
        bachelorArgs.append(" -include_vcf_filtered");
        bachelorArgs.append(String.format(" -germline_vcf %s/%s", VmDirectories.INPUT, germlineVcfFile));
        bachelorArgs.append(String.format(" -xml_config %s/%s", VmDirectories.INPUT, BACHELOR_XML_CONFIG));
        bachelorArgs.append(String.format(" -ext_filter_file %s/%s", VmDirectories.INPUT, CLINVAR_FILTERS));
        bachelorArgs.append(String.format(" -ref_genome %s", refGenome));
        bachelorArgs.append(String.format(" -tumor_bam_file %s/%s", VmDirectories.INPUT, tumorBamFile));
        bachelorArgs.append(String.format(" -output_dir %s/", VmDirectories.OUTPUT));
        bachelorArgs.append(String.format(" -log_debug"));

        startupScript.addCommand(() -> format("java -jar %s %s", bachelorJar, bachelorArgs.toString()));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "bachelor"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("bachelor-wide").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(250)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("BachelorCohort", "Run Bachelor on cohort",
                OperationDescriptor.InputType.FLAT);
    }

}
