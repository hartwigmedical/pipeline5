package com.hartwig.pipeline.calling.structural.gridss.command;

public class AnnotateInsertedSequence extends GridssCommand {

    public AnnotateInsertedSequence(final String inputVcf, final String referenceGenome, final String outputVcf) {
        super("gridss.AnnotateInsertedSequence",
                "8G",
                "REFERENCE_SEQUENCE=" + referenceGenome,
                "INPUT=" + inputVcf,
                "OUTPUT=" + outputVcf);
    }
}
