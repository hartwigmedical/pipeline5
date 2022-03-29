package com.hartwig.pipeline.metrics;

import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class BedToIntervalsCommand extends JavaClassCommand {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    BedToIntervalsCommand(final String inputBed, final String outputIntervals, final String referenceFasta) {
        super("gridss",
                Versions.GRIDSS,
                "gridss.jar",
                "picard.cmdline.PicardCommandLine",
                "1G",
                "BedToIntervalList",
                "SORT=true",
                "SEQUENCE_DICTIONARY=" + referenceFasta.replace("fna", "dict"),
                "INPUT=" + inputBed,
                "OUTPUT=" + outputIntervals);
    }
}