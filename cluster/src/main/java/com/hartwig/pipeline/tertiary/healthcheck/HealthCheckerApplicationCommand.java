package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class HealthCheckerApplicationCommand extends JavaJarCommand {

    HealthCheckerApplicationCommand(String referenceSampleName, String tumorSampleName, String referenceMetricsPath, String tumorMetricsPath,
            String referenceFlagstatPath, String tumorFlagstatPath, String purplePath, String outputPath) {
        super("health-checker",
                Versions.HEALTH_CHECKER,
                "health-checker.jar",
                "10G",
                Lists.newArrayList("-reference",
                        referenceSampleName,
                        "-tumor",
                        tumorSampleName,
                        "-ref_wgs_metrics_file",
                        referenceMetricsPath,
                        "-tum_wgs_metrics_file",
                        tumorMetricsPath,
                        "-ref_flagstat_file",
                        referenceFlagstatPath,
                        "-tum_flagstat_file",
                        tumorFlagstatPath,
                        "-purple_dir",
                        purplePath,
                        "-output_dir",
                        outputPath));
    }
}
