package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import static com.hartwig.batch.operations.rna.RnaCommon.getRnaCohortDirectory;
import static com.hartwig.batch.operations.rna.RnaCommon.getRnaResourceDirectory;
import static com.hartwig.pipeline.resource.RefGenomeVersion.RG_37;

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
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class RnaStarMapping implements BatchOperation {

    private static final String STAR_DIR = "star";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {

        InputFileDescriptor descriptor = inputs.get();

        final String batchInputs = descriptor.inputValue();
        final String[] batchItems = batchInputs.split(",");

        // required format: SampleId,RefGenomeVersion (HG37 by default),FASTA file bucket

        /*
        if(batchItems.length != 2)
        {
            System.out.print(String.format("invalid input arguments(%s) - expected SampleId,RefGenomeVersion,FastqFileBucketDir", batchInputs));
            return null;
        }
        */

        final String sampleId = batchItems[0];
        final RefGenomeVersion refGenomeVersion = batchItems.length >= 2 ? RefGenomeVersion.valueOf(batchItems[1]) : RG_37;

        if(batchItems.length >= 3) {
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
        }
        else
        {
            // expected location: "gs://cpct02010255tii-rna-reads/1.3/CPCT02010255TII_AHWGLNBGX5_S4_L002_R1_001.fastq.gz
            final String sampleFastqFiles = String.format("gs://%s-rna-reads/1.3/*.fastq.gz", sampleId.toLowerCase());
            startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", sampleFastqFiles, VmDirectories.INPUT));
        }

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("$(ls %s/*_R1* | tr '\\n' ',')", VmDirectories.INPUT);
        final String r2Files = format("$(ls %s/*_R2* | tr '\\n' ',')", VmDirectories.INPUT);

        // copy reference files for STAR
        final String starGenomeDir = getRnaResourceDirectory(refGenomeVersion, STAR_DIR);
        final String localStarGenomeDir = String.format("%s/%s", VmDirectories.INPUT, STAR_DIR);

        startupScript.addCommand(() -> format("mkdir %s", localStarGenomeDir));
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s/* %s", starGenomeDir, localStarGenomeDir));

        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run the STAR mapper
        final String[] starArgs = {"--runThreadN", threadCount, "--genomeDir", localStarGenomeDir, "--genomeLoad", "NoSharedMemory",
                "--readFilesIn", r1Files, r2Files, "--readFilesCommand", "zcat", "--outSAMtype", "BAM", "Unsorted",
                "--outSAMunmapped", "Within", "--outBAMcompression", "0", "--outSAMattributes", "All",
                "--outFilterMultimapNmax", "10", "--outFilterMismatchNmax", "3", "limitOutSJcollapsed", "3000000",
                "--chimSegmentMin", "10", "--chimOutType", "WithinBAM", "SoftClip", "--chimJunctionOverhangMin", "10", "--chimSegmentReadGapMax", "3",
                "--chimScoreMin", "1", "--chimScoreDropMax", "30", "--chimScoreJunctionNonGTAG", "0", "--chimScoreSeparation", "1",
                "--outFilterScoreMinOverLread", "0.33", "--outFilterMatchNminOverLread", "0.33", "--outFilterMatchNmin", "35",
                "--alignSplicedMateMapLminOverLmate", "0.33", "--alignSplicedMateMapLmin", "35", "--alignSJstitchMismatchNmax", "5", "-1", "5", "5"};

        startupScript.addCommand(new VersionedToolCommand("star", "STAR", "2.7.3a", starArgs));

        final String bamFile = "Aligned.out.bam";

        // sort the BAM
        final String sortedBam = sampleId + ".sorted.bam";

        final String[] sortArgs = {"sort", "-@", threadCount, "-m", "2G", "-T", "tmp", "-O", "bam", bamFile, "-o", sortedBam};

        startupScript.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, sortArgs));

        // mark duplicate fragment reads within the BAM
        final String sortedDedupedBam = sampleId + ".sorted.dups.bam";

        final String[] dupArgs = {"markdup", "-t", threadCount, "--overflow-list-size=45000000", sortedBam, sortedDedupedBam};

        startupScript.addCommand(new SambambaCommand(dupArgs));

        final String[] indexArgs = {"index", sortedDedupedBam};

        startupScript.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, indexArgs));

        // clean up intermediary BAMs
        startupScript.addCommand(() -> format("rm -f %s", bamFile));
        startupScript.addCommand(() -> format("rm -f %s", sortedBam));

        final String starStats = "Log.final.out";
        final String statsFile = sampleId + "." + starStats;
        startupScript.addCommand(() -> format("mv %s %s", starStats, statsFile));

        // run QC stats on the fast-Qs as well
        /*
        final String fastqcOutputDir = format("%s/fastqc", VmDirectories.OUTPUT);
        startupScript.addCommand(() -> format("mkdir %s", fastqcOutputDir));

        final String allFastQs = format("%s/*gz", VmDirectories.INPUT);
        final String[] fastqcArgs = {"-o", fastqcOutputDir, allFastQs};

        // TEMP until reimage has taken place
        // startupScript.addCommand(() -> format("chmod a+x /opt/tools/fastqc/0.11.4/fastqc"));

        startupScript.addCommand(new VersionedToolCommand("fastqc", "fastqc", "0.11.4", fastqcArgs));
        */

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "star"), executionFlags));

        // copy results to crunch

        final String resultsDir = getRnaCohortDirectory(refGenomeVersion);

        startupScript.addCommand(() -> format("gsutil -m cp %s/* %s/%s/", VmDirectories.OUTPUT, resultsDir, sampleId));

        // startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of("rna-cohort", sampleId), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-star-mapping").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(500)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 48)).build();
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
        return OperationDescriptor.of("RnaStarMapping", "Generate BAMs from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
