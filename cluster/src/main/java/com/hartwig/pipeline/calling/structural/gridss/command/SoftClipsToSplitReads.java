package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class SoftClipsToSplitReads {
    private final static String CLASS_NAME = "gridss.SoftClipsToSplitReads";

    private static GridssArguments sharedArguments(final String inputBam, final String outputBam,
                                                   final String referenceGenome) {
        return new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("i", inputBam)
                .add("o", outputBam);
    }

    public static class ForPreprocess implements GridssCommand {
        private final String intermediateBam;
        private final String referenceGenome;
        private final String outputBam;

        public ForPreprocess(final String intermediateBam, final String referenceGenome, final String outputBam) {
            this.intermediateBam = intermediateBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String arguments() {
            return sharedArguments(intermediateBam, outputBam, referenceGenome).asBash();
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

        public ForAssemble(final String inputBam, final String referenceGenome, final String outputBam) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public String arguments() {
            return sharedArguments(inputBam, outputBam, referenceGenome)
                .add("realign_entire_read", "true").asBash();
        }

        @Override
        public String className() {
            return CLASS_NAME;
        }
    }
}
