package com.hartwig.pipeline.calling.structural.gridss.command;

public class AnnotateUntemplatedSequence extends GridssCommand {

    public AnnotateUntemplatedSequence(final String inputVcf, final String referenceGenome, final String outputVcf) {
        super("gridss.AnnotateUntemplatedSequence",
                "8G",
                "REFERENCE_SEQUENCE=" + referenceGenome,
                "INPUT=" + inputVcf,
                "OUTPUT=" + outputVcf);
    }
}
