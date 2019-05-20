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
                .add("'aligner_command_line", format("-K %d'", GridssCommon.GRIDSS_BWA_BASES_PER_BATCH))
                .add("aligner_command_line", "-t")
                .add("'aligner_command_line", "%3$d'")
                .add("'aligner_command_line", "%2$s'")
                .add("'aligner_command_line", "%1$s'");
    }

    public static class ForPreprocess implements BashCommand {
        private String intermediateBam;
        private String referenceGenome;
        private String outputBam;

        public ForPreprocess(String intermediateBam, String referenceGenome, String outputBam) {
            this.intermediateBam = intermediateBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String asBash() {
            return GridssCommon.gridssCommand(CLASS_NAME, "4G", asList("-Dgridss.output_to_temp_file=true"),
                    sharedArguments(intermediateBam, outputBam, referenceGenome, 2).asBash())
                    .asBash();
        }
    }

    public static class ForAssemble implements BashCommand {
        private final String inputBam;
        private final String referenceGenome;
        private final String outputBam;

        public ForAssemble(String inputBam, String referenceGenome, String outputBam) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String asBash() {
            String gridssArgs = sharedArguments(inputBam, outputBam, referenceGenome, 2)
                .add("realign_entire_read", "true").asBash();
            return GridssCommon.gridssCommand(CLASS_NAME, "8G",
                    asList("-Dgridss.async.buffersize=16", "-Dgridss.output_to_temp_file=true"),
                    gridssArgs).asBash();
        }
    }
}
