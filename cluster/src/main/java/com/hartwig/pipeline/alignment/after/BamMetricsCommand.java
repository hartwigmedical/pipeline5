package com.hartwig.pipeline.alignment.after;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.util.Arrays.asList;

public class BamMetricsCommand extends JavaJarCommand {
    public BamMetricsCommand(String inputBam, String referenceFasta, Sample sample) {
        super("picard", "2.18.27", "picard.jar", "24G",
                asList(
                "CollectWgsMetrics",
                "REFERENCE_SEQUENCE=" + referenceFasta,
                "INPUT=" + inputBam,
                "OUTPUT=" + String.format("%s/%s.wgsmetrics", VmDirectories.OUTPUT, sample.name()),
                "MINIMUM_MAPPING_QUALITY=20",
                "MINIMUM_BASE_QUALITY=10",
                "COVERAGE_CAP=250"
        ));
    }

}
