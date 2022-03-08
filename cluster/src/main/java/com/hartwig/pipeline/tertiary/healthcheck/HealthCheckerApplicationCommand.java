package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class HealthCheckerApplicationCommand extends JavaJarCommand {

    HealthCheckerApplicationCommand(
            final String referenceSampleName, final String tumorSampleName, final String referenceMetricsPath, final String tumorMetricsPath,
            final String referenceFlagstatPath, final String tumorFlagstatPath, final String purplePath, final String outputPath) {
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
