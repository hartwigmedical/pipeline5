package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class SoftClipsToSplitReads {
    private final static String CLASS_NAME = "gridss.SoftClipsToSplitReads";

    private static GridssArguments sharedArguments(String inputBam, String outputBam,
                                                   String referenceGenome, int workerThreads) {
        return new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("i", inputBam)
                .add("o", outputBam)
                .add("worker_threads", String.valueOf(workerThreads))
                .add("aligner_command_line", "null")
                .add("aligner_command_line", GridssCommon.pathToBwa())
                .add("aligner_command_line", "mem")
                .add("aligner_command_line", "-K " + GridssCommon.GRIDSS_BWA_BASES_PER_BATCH)
                .add("aligner_command_line", "-t")
                .add("'aligner_command_line", "%3$d'")
                .add("'aligner_command_line", "%2$s'")
                .add("'aligner_command_line", "%1$s'");
    }

    public static class ForPreprocess implements BashCommand {
        private String inputBam;
        private String referenceGenome;

        public ForPreprocess(String inputBam, String referenceGenome) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
        }

        @Override
        public String asBash() {
            return GridssCommon.gridssCommand(CLASS_NAME, "4G", asList("-Dgridss.output_to_temp_file=true"),
                    sharedArguments(inputBam, resultantBam(), referenceGenome, 2).asBash())
                    .asBash();
        }

        public String resultantBam() {
            return format("%s/preprocess-soft_clips-to-split_reads.bam", VmDirectories.OUTPUT);
        }
    }

    public static class ForAssemble implements BashCommand {
        private final String inputBam;
        private final String referenceGenome;

        public ForAssemble(String inputBam, String referenceGenome) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
        }

        @Override
        public String asBash() {
            String gridssArgs = sharedArguments(inputBam, resultantBam(), referenceGenome, 2)
                .add("realign_entire_read", "true").asBash();
            return GridssCommon.gridssCommand(CLASS_NAME, "8G",
                    asList("-Dgridss.async.buffersize=16", "-Dgridss.output_to_temp_file=true"),
                    gridssArgs).asBash();
        }

        public String resultantBam() {
            return format("%s/assemble-soft_clips-to-split_reads.bam", VmDirectories.OUTPUT);
        }
    }
}
