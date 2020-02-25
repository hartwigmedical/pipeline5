package com.hartwig.pipeline.calling.somatic;

class SageV2PostProcessCommand extends SageCommand {
    SageV2PostProcessCommand(final String assembly, final String intputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SagePostProcessApplication", "8G", "-in", intputVcf, "-out", outputVcf, "-assembly", assembly);
    }
}
