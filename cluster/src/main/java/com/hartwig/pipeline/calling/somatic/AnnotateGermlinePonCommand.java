package com.hartwig.pipeline.calling.somatic;

class AnnotateGermlinePonCommand extends BcfToolsCommand {
    AnnotateGermlinePonCommand(final String germlinePon, final String inputVcf, final String outputVcf) {
        super("annotate", "-a", germlinePon, "-c", "GERMLINE_PON_COUNT", "-o", outputVcf, "-O", "z", inputVcf);
    }
}
