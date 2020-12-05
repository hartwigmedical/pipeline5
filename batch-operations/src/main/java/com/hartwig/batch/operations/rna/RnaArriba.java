package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.RNA_COHORT_LOCATION_HG37;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_RESOURCES;

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
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class RnaArriba implements BatchOperation
{
    private static final String ARRIBA = "arriba";
    private static final String ARRIBA_TOOL = "arriba";
    private static final String ARRIBA_RESOURCES = String.format("%s/%s", RNA_RESOURCES, ARRIBA);
    private static final String RNA_BAM_FILE_ID = ".sorted.dups.bam";
    private static final String RNA_BAM_INDEX_FILE_ID = ".sorted.dups.bam.bai";

    private static final String REF_GENOME = "hs37d5.fa";
    private static final String GENE_DEFINITIONS = "GENCODE19.gtf";
    private static final String BLACKLIST = "blacklist_hg19_hs37d5_GRCh37_2018-11-04.tsv.gz";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.inputValue();

        final String sampleId = batchInputs;

        // copy down BAM and index file for this sample
        final String bamFile = String.format("%s%s", sampleId, RNA_BAM_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RNA_COHORT_LOCATION_HG37, sampleId, bamFile, VmDirectories.INPUT));

        final String bamIndexFile = String.format("%s%s", sampleId, RNA_BAM_INDEX_FILE_ID);
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s %s",
                RNA_COHORT_LOCATION_HG37, sampleId, bamIndexFile, VmDirectories.INPUT));

        // copy down the executable
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", ARRIBA_RESOURCES, ARRIBA_TOOL, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("chmod a+x %s/%s", VmDirectories.TOOLS, ARRIBA_TOOL));

        // copy down required reference files
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s", ARRIBA_RESOURCES, REF_GENOME, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", ARRIBA_RESOURCES, GENE_DEFINITIONS, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s %s", ARRIBA_RESOURCES, BLACKLIST, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run Arriba
        StringBuilder arribaArgs = new StringBuilder();
        arribaArgs.append(String.format(" -x %s/%s", VmDirectories.INPUT, bamFile));
        arribaArgs.append(String.format(" -o %s/%s.fusions.tsv", VmDirectories.OUTPUT, sampleId));
        arribaArgs.append(String.format(" -O %s/%s.fusions.discarded.tsv", VmDirectories.OUTPUT, sampleId));
        arribaArgs.append(String.format(" -a %s/%s", VmDirectories.INPUT, REF_GENOME));
        arribaArgs.append(String.format(" -g %s/%s", VmDirectories.INPUT, GENE_DEFINITIONS));
        arribaArgs.append(String.format(" -b %s/%s", VmDirectories.INPUT, BLACKLIST));
        arribaArgs.append(" -T -P");

        startupScript.addCommand(() -> format("%s/%s %s", VmDirectories.TOOLS, ARRIBA_TOOL, arribaArgs.toString()));

        /*
            ./tools/arriba_v1.1.0/arriba
            -x ./runs/CPCT02020378T/CPCT02020378T.sorted.bam
            -o ./runs/CPCT02020378T/fusions.tsv -O
            ./runs/CPCT02020378T/fusions.discarded.tsv
            -a "./ref/hs37d5_GENCODE19/hs37d5.fa"
            -g "./ref/hs37d5_GENCODE19/GENCODE19.gtf"
            -b "./tools/arriba_v1.1.0/database/blacklist_hg19_hs37d5_GRCh37_2018-11-04.tsv.gz"
            -T -P
         */

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "arriba"), executionFlags));

        // copy results to rna-analysis location on crunch
        startupScript.addCommand(() -> format("gsutil -m cp %s/* %s/%s/arriba/", VmDirectories.OUTPUT, RNA_COHORT_LOCATION_HG37, sampleId));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-arriba").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(100)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 64)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaArriba", "Run Arriba fusion calling",
                OperationDescriptor.InputType.FLAT);
    }
}
