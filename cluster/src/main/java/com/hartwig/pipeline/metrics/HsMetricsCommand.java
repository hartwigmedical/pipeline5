package com.hartwig.pipeline.metrics;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class HsMetricsCommand extends JavaClassCommand {
    HsMetricsCommand(final String inputBam, final String referenceFasta, final String baitIntervalsList, final String targetIntervalsList,
            final String outputFile) {
        super("gridss",
                Versions.GRIDSS,
                "gridss.jar",
                "picard.cmdline.PicardCommandLine",
                "24G",
                "CollectHsMetrics",
                "REFERENCE_SEQUENCE=" + referenceFasta,
                "INPUT=" + inputBam,
                "OUTPUT=" + outputFile,
                "BAIT_INTERVALS=" + baitIntervalsList,
                "TARGET_INTERVALS=" + targetIntervalsList);
    }
}