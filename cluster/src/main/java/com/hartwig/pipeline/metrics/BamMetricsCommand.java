package com.hartwig.pipeline.metrics;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.tools.Versions;

class BamMetricsCommand extends JavaClassCommand {
    BamMetricsCommand(final String inputBam, final String referenceFasta, final String outputFile) {
        super("gridss",
                Versions.GRIDSS,
                "gridss.jar",
                "picard.cmdline.PicardCommandLine",
                "24G",
                Lists.newArrayList("-Dsamjdk.use_async_io_read_samtools=true",
                        "-Dsamjdk.use_async_io_write_samtools=true",
                        "-Dsamjdk.use_async_io_write_tribble=true",
                        "-Dsamjdk.buffer_size=4194304"),
                "CollectWgsMetrics",
                "REFERENCE_SEQUENCE=" + referenceFasta,
                "INPUT=" + inputBam,
                "OUTPUT=" + outputFile,
                "MINIMUM_MAPPING_QUALITY=20",
                "MINIMUM_BASE_QUALITY=10",
                "COVERAGE_CAP=250");
    }
}