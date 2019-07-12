package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class SoftClipsToSplitReads {
    private final static String CLASS_NAME = "gridss.SoftClipsToSplitReads";

    private static List<GridssArgument> sharedArguments(final String inputBam, final String outputBam,
                                                   final String referenceGenome) {
        return Arrays.asList(GridssArgument.tempDir(),
                new GridssArgument("working_dir", VmDirectories.OUTPUT),
                new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("i", inputBam),
                new GridssArgument("o", outputBam));
    }

    public static class ForPreprocess extends GridssCommand {
        private final String intermediateBam;
        private final String referenceGenome;
        private final String outputBam;

        public ForPreprocess(final String intermediateBam, final String referenceGenome, final String outputBam) {
            this.intermediateBam = intermediateBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public List<GridssArgument> arguments() {
            return sharedArguments(intermediateBam, outputBam, referenceGenome);
        }

        @Override
        public String className() {
            return CLASS_NAME;
        }
    }

    public static class ForAssemble extends GridssCommand {
        private final String inputBam;
        private final String referenceGenome;
        private final String outputBam;

        public ForAssemble(final String inputBam, final String referenceGenome, final String outputBam) {
            this.inputBam = inputBam;
            this.referenceGenome = referenceGenome;
            this.outputBam = outputBam;
        }

        @Override
        public List<GridssArgument> arguments() {
            List<GridssArgument> arguments = new ArrayList<>(sharedArguments(inputBam, outputBam, referenceGenome));
            arguments.add(new GridssArgument("realign_entire_read", "true"));
            return arguments;
        }

        @Override
        public String className() {
            return CLASS_NAME;
        }
    }

    public static final void main(String[] args) {
        System.out.println(new SoftClipsToSplitReads.ForAssemble("input", "refgen", "output").asBash());
    }
}
