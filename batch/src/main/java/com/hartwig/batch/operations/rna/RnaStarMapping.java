package com.hartwig.batch.operations.rna;

import static java.lang.String.format;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class RnaStarMapping implements BatchOperation {

    private static final String REF_GENCODE_37 = "/opt/resources/hs37d5_GENCODE19";

    @Override
    public VirtualMachineJobDefinition execute(
            InputBundle inputs, RuntimeBucket bucket, BashStartupScript startupScript, RuntimeFiles executionFlags) {
        InputFileDescriptor descriptor = inputs.get();

        final String sampleId = descriptor.value();

        // TMP: enable STAR for execution
        startupScript.addCommand(() -> "chmod a+x /opt/tools/star/2.7.3a/STAR");

        // copy down FASTQ files for this sample
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s", "gs://rna-fastqs/*/*.gz", VmDirectories.INPUT));

        // locate the FASTQ files for reads 1 and 2
        final String r1Files = format("$(ls %s/*_R1* | tr '\\n' ',')", VmDirectories.INPUT);
        final String r2Files = format("$(ls %s/*_R2* | tr '\\n' ',')", VmDirectories.INPUT);

        // logging
        final String threadCount = Bash.allCpus();

        startupScript.addCommand(() -> format("cd %s", VmDirectories.OUTPUT));

        // run the STAR mapper
        final String[] starArgs = {"--runThreadN", threadCount, "--genomeDir", REF_GENCODE_37, "--genomeLoad", "NoSharedMemory",
                "--readFilesIn", r1Files, r2Files, "--readFilesCommand", "zcat", "--outSAMtype", "BAM", "Unsorted",
                "--outSAMunmapped", "Within", "--outBAMcompression", "0", "--outSAMattributes", "All",
                "--outFilterMultimapNmax", "1", "--outFilterMismatchNmax", "3", "limitOutSJcollapsed", "3000000",
                "--chimSegmentMin", "10", "--chimOutType", "WithinBAM", "SoftClip", "--chimJunctionOverhangMin", "10",
                "--chimScoreMin", "1", "--chimScoreDropMax", "30", "--chimScoreJunctionNonGTAG", "0", "--chimScoreSeparation", "1",
                "--alignSJstitchMismatchNmax", "5", "-1", "5", "5", "--chimSegmentReadGapMax", "3"};

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

        // run QC stats on the fast-Qs as well
        final String fastqcOutputDir = format("%s/fastqc", VmDirectories.OUTPUT);
        startupScript.addCommand(() -> format("mkdir %s", fastqcOutputDir));

        final String allFastQs = format("%s/*gz", VmDirectories.INPUT);
        final String[] fastqcArgs = {"-o", fastqcOutputDir, allFastQs};
        startupScript.addCommand(new VersionedToolCommand("fastqc", "fastqc", "0.11.4", fastqcArgs));

        // upload the results
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), "star"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder().name("rna-star-mapping").startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory()).workingDiskSpaceGb(650)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(12, 36)).build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("RnaStarMapping", "Generate BAMs from RNA FASTQs",
                OperationDescriptor.InputType.FLAT);
    }

}
