package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.computeengine.execution.vm.Bash;

import java.util.Collections;

public class AnnotateInsertedSequence extends GridssCommand {

    public static GridssCommand viralAnnotation(final String inputVcf, final String outputVcf, final String viralReference) {
        return new AnnotateInsertedSequence(inputVcf, outputVcf, viralReference, Alignment.APPEND, "");
    }

    private enum Alignment {
        APPEND,
        REPLACE
    }

    private AnnotateInsertedSequence(final String inputVcf, final String outputVcf, final String referencesSequence,
                                     final Alignment alignment, final String additional) {
        super("gridss.AnnotateInsertedSequence",
                "8G",
                Collections.emptyList(),
                "REFERENCE_SEQUENCE=" + referencesSequence,
                "INPUT=" + inputVcf,
                "OUTPUT=" + outputVcf,
                "ALIGNMENT=" + alignment.toString(),
                "WORKER_THREADS=" + Bash.allCpus(),
                additional);
    }
}