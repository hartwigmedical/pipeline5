package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class HealthCheckerApplicationCommand extends JavaJarCommand {

    HealthCheckerApplicationCommand(String referenceSampleName, String tumorSampleName, String metricsPath, String amberPath,
            String purplePath, String outputPath) {
        super("health-checker",
                Versions.HEALTH_CHECKER,
                "health-checker.jar",
                "10G",
                Lists.newArrayList("-reference",
                        referenceSampleName,
                        "-tumor",
                        tumorSampleName,
                        "-metrics_dir",
                        metricsPath,
                        "-amber_dir",
                        amberPath,
                        "-purple_dir",
                        purplePath,
                        "-output_dir",
                        outputPath));
    }
}
