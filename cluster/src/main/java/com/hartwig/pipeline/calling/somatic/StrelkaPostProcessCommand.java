package com.hartwig.pipeline.calling.somatic;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;

class StrelkaPostProcessCommand extends JavaJarCommand {

    StrelkaPostProcessCommand(final String inputVcf, final String outputVcf, final String highConfidenceBed, final String tumorSample,
            final String tumorBamPath) {
        super("strelka-post-process",
                "1.4",
                "strelka-post-process.jar",
                "20G",
                Lists.newArrayList("-v", inputVcf, "-hc_bed", highConfidenceBed, "-t", tumorSample, "-o", outputVcf, "-b", tumorBamPath));
    }
}
