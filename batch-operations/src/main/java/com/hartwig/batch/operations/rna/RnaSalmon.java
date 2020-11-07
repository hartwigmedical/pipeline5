package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.RNA_COHORT_LOCATION;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_RESOURCES;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
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
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class RnaSalmon implements BatchOperation {

    private static final String SALMON = "salmon";
    private static final String SALMON_BINARY = "salmon/bin/salmon";
    private static final String SALMON_INDEX_DIR = "salmon_gene_index";

    private static final String SALMON_RESOURCES = String.format("%s/%s", RNA_RESOURCES, SALMON);

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.inputValue();
        final String[] batchItems = batchInputs.split(",");

        if(batchItems.length != 2)
        {
            System.out.print(String.format("invalid input arguments(%s) - expected SampleId,PathToFastqFiles", batchInputs));
            return null;
        }

        final String sampleId = batchItems[0];
        final String fastqFilelist = batchItems[1];

        final List<String> sampleFastqFiles = getSampleFastqFileList(sampleId, fastqFilelist);

        if(sampleFastqFiles.isEmpty()) {
            System.out.print(String.format("sampleId(%s) fastq files not found", sampleId));
            return null;
        }

        // copy down FASTQ files for this sample
        for(final String fastqFile : sampleFastqFiles)
        {
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", fastqFile, VmDirectories.INPUT));
        }

        // copy down the executable
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s",
                SALMON_RESOURCES, SALMON, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("chmod a+x %s/%s", VmDirectories.TOOLS, SALMON_BINARY));

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("%s/*R1_001.fastq.gz", VmDirectories.INPUT);
        final String r2Files = format("%s/*R2_001.fastq.gz", VmDirectories.INPUT);

        // copy reference files for SALMON
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s",
                SALMON_RESOURCES, SALMON_INDEX_DIR, VmDirectories.INPUT));

        final String salmonGeneIndexDir = String.format("%s/%s", VmDirectories.INPUT, SALMON_INDEX_DIR);

        // logging
        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        /*
        genome_ref=$ref_root/salmon_gene_ihs37d5_GENCODE19ndex
        READ1=${input_dir}/*R1_001.fastq.gz
        READ2=${input_dir}/*R2_001.fastq.gz
        threads=6

        ${salmon} quant \
          -i ${genome_ref} \
          -l A -1 ${READ1} -2 ${READ2} \
          -p ${threads} --validateMappings \
          -o ${output_dir}
         */

        // run the STAR mapper
        StringBuilder salmonArgs = new StringBuilder();
        salmonArgs.append("quant");
        salmonArgs.append(String.format(" -i %s", salmonGeneIndexDir));
        salmonArgs.append(" -l A");
        salmonArgs.append(String.format(" -1 %s", r1Files));
        salmonArgs.append(String.format(" -2 %s", r2Files));
        salmonArgs.append(String.format(" -p %s", threadCount));
        salmonArgs.append(" --validateMappings");
        salmonArgs.append(String.format(" -o %s", VmDirectories.OUTPUT));

        startupScript.addCommand(() -> format("%s/%s %s", VmDirectories.TOOLS, SALMON_BINARY, salmonArgs.toString()));

        final String rawOutputFile = "quant.sf";
        final String outputFile = sampleId + ".salmon.tsv";

        startupScript.addCommand(() -> format("mv %s %s", rawOutputFile, outputFile));

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "salmon"), executionFlags));

        // copy results to rna-analysis location on crunch
        startupScript.addCommand(() -> format("gsutil -m cp %s/* %s/%s/salmon/", VmDirectories.OUTPUT, RNA_COHORT_LOCATION, sampleId));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-salmon").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(500)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 36)).build();
    }

    private final List<String> getSampleFastqFileList(final String sampleId, final String fastqFilelist)
    {
        final List<String> fileList = Lists.newArrayList();

        if (!Files.exists(Paths.get(fastqFilelist)))
        {
            return fileList;
        }

        try
        {
            final List<String> fileContents = Files.readAllLines(new File(fastqFilelist).toPath());

            if(fileContents.isEmpty())
                return fileList;

            final String sampleIdStr = sampleId + "_"; // to avoid TII samples matching

            fileList.addAll(fileContents.stream().filter(x -> x.contains(sampleIdStr)).collect(Collectors.toList()));
        }
        catch (IOException e)
        {
        }

        return fileList;
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaSalmon", "Run Salmom to calculate TPM from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
