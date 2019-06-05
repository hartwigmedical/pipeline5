package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

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
                .add("'aligner_command_line", format("-K %d'", GridssCommon.GRIDSS_BWA_BASES_PER_BATCH))
                .add("aligner_command_line", "-t")
                .add("'aligner_command_line", "%3$d'")
                .add("'aligner_command_line", "%2$s'")
                .add("'aligner_command_line", "%1$s'");
    }

    public static class ForPreprocess implements GridssCommand {
        private String intermediateBam;
        private String referenceGenome;
        private String outputBam;

        public ForPreprocess(String intermediateBam, String referenceGenome, String outputBam) {
            this.intermediateBam = intermediateBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String arguments() {
            return sharedArguments(intermediateBam, outputBam, referenceGenome, 2).asBash();
        }

        @Override
        public String className() {
            return CLASS_NAME;
        }
    }

    public static class ForAssemble implements GridssCommand {
        private final String inputBam;
        private final String referenceGenome;
        private final String outputBam;

        public ForAssemble(String inputBam, String referenceGenome, String outputBam) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String arguments() {
            return sharedArguments(inputBam, outputBam, referenceGenome, 2)
                .add("realign_entire_read", "true").asBash();
        }

        @Override
        public String className() {
            return CLASS_NAME;
        }
    }
}
