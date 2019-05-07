package com.hartwig.pipeline.bammetrics;

import com.hartwig.pipeline.execution.vm.JavaJarCommand;

import static java.util.Arrays.asList;

public class BamMetricsCommand extends JavaJarCommand {
    public BamMetricsCommand(String inputBam, String referenceFasta, final String outputFile) {
        super("picard", "2.18.27", "picard.jar", "24G",
                asList(
                "CollectWgsMetrics",
                "REFERENCE_SEQUENCE=" + referenceFasta,
                "INPUT=" + inputBam,
                "OUTPUT=" + outputFile,
                "MINIMUM_MAPPING_QUALITY=20",
                "MINIMUM_BASE_QUALITY=10",
                "COVERAGE_CAP=250"
        ));
    }

}
