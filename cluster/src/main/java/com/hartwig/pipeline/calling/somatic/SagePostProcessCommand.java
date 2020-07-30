package com.hartwig.pipeline.calling.somatic;

class SagePostProcessCommand extends SageCommand {
    SagePostProcessCommand(final String assembly, final String intputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SagePostProcessApplication", "8G", "-in", intputVcf, "-out", outputVcf, "-assembly", assembly);
    }
}
