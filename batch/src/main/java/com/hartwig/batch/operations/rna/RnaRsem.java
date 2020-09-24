package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
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

public class RnaRsem implements BatchOperation {

    private static final String RSEM_TOOL = "rsem";
    private static final String RSEM_EXPRESSION_CMD = "rsem-calculate-expression";
    private static final String RSEM_GENE_INDEX_DIR = "rsem_gene_index";
    private static final String RSEM_GENE_INDEX = "human_gencode";

    private static final String REF_GENCODE_37 = "hs37d5_GENCODE19";
    private static final String REF_LOCATION = "gs://isofox-resources";

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

        // download the executables
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s",
                REF_LOCATION, RSEM_TOOL, VmDirectories.TOOLS));

        startupScript.addCommand(() -> format("chmod a+x %s/%s/*", VmDirectories.TOOLS, RSEM_TOOL));

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("$(ls %s/*_R1* | tr '\\n' ',')", VmDirectories.INPUT);
        final String r2Files = format("$(ls %s/*_R2* | tr '\\n' ',')", VmDirectories.INPUT);

        // download reference files
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s", REF_LOCATION, REF_GENCODE_37, VmDirectories.INPUT));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp -r %s/%s %s", REF_LOCATION, RSEM_GENE_INDEX_DIR, VmDirectories.INPUT));

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run STAR with transcriptome mapping
        final String refGenomeDir = String.format("%s/%s", VmDirectories.INPUT, REF_GENCODE_37);

        final String threadCount = Bash.allCpus();

        final String[] starArgs = {"--runThreadN", threadCount, "--genomeDir", refGenomeDir, "--genomeLoad", "NoSharedMemory",
                "--readFilesIn", r1Files, r2Files, "--readFilesCommand", "zcat", "--outSAMtype", "BAM", "Unsorted",
                "--outSAMunmapped", "Within", "--outBAMcompression", "0", "--outSAMattributes", "All",
                "--outFilterMultimapNmax", "10", "--outFilterMismatchNmax", "3", "limitOutSJcollapsed", "3000000",
                "--chimSegmentMin", "10", "--chimOutType", "WithinBAM", "SoftClip", "--chimJunctionOverhangMin", "10", "--chimSegmentReadGapMax", "3",
                "--chimScoreMin", "1", "--chimScoreDropMax", "30", "--chimScoreJunctionNonGTAG", "0", "--chimScoreSeparation", "1",
                "--outFilterScoreMinOverLread", "0.33", "--outFilterMatchNminOverLread", "0.33", "--outFilterMatchNmin", "35",
                "--alignSplicedMateMapLminOverLmate", "0.33", "--alignSplicedMateMapLmin", "35", "--alignSJstitchMismatchNmax", "5", "-1", "5", "5",
                "--quantMode", "TranscriptomeSAM"}; // key line for RSEM;

        startupScript.addCommand(new VersionedToolCommand("star", "STAR", "2.7.3a", starArgs));

        // key output file is 'Aligned.toTranscriptome.out.bam'

        // ./tools/RSEM-1.3.3/rsem-calculate-expression --alignments --paired-end
        // ./runs/CPCT02020378T/Aligned.toTranscriptome.out.bam
        // ./ref/rsem_gene_index/human_gencode
        // CPCT02020378T.rsem -p 6 &

        final String transcriptomeBam = "Aligned.toTranscriptome.out.bam";

        // TMP: copy transcriptome BAM to the bucket
        // startupScript.addCommand(() -> format("gsutil -m cp %s/%s gs://rna-cohort/%s/rsem/", VmDirectories.OUTPUT, transcriptomeBam, sampleId));

        final String rsemGeneIndex = String.format("%s/%s/%s", VmDirectories.INPUT, RSEM_GENE_INDEX_DIR, RSEM_GENE_INDEX);
        final String outputPrefix = String.format("%s.rsem", sampleId);

        // run RSEM
        StringBuilder rsemArgs = new StringBuilder();
        rsemArgs.append(" --alignments");
        rsemArgs.append(" --paired-end");
        rsemArgs.append(String.format(" %s", transcriptomeBam));
        rsemArgs.append(String.format(" %s", rsemGeneIndex));
        rsemArgs.append(String.format(" %s", outputPrefix));
        rsemArgs.append(String.format(" -p %s", threadCount));

        // run RSEM transcript expression calcs
        startupScript.addCommand(() -> format("%s/%s/%s %s", VmDirectories.TOOLS, RSEM_TOOL, RSEM_EXPRESSION_CMD, rsemArgs.toString()));

        startupScript.addCommand(() -> format("mv %s.rsem.genes.results %s.rsem.gene_data.tsv", sampleId, sampleId));
        startupScript.addCommand(() -> format("mv %s.rsem.isoforms.results %s.rsem.trans_data.tsv", sampleId, sampleId));

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "rsem"), executionFlags));

        // copy results to rna-analysis location on crunch
        startupScript.addCommand(() -> format("gsutil -m cp %s/*tsv gs://rna-cohort/%s/rsem/", VmDirectories.OUTPUT, sampleId));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-rsem").startupCommand(startupScript)
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
        return OperationDescriptor.of("RnaRsem", "Run RSEM to calculate TPM from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
