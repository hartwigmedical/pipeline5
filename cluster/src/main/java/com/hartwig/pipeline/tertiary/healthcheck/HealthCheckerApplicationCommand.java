package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class HealthCheckerApplicationCommand extends JavaJarCommand {

    HealthCheckerApplicationCommand(String runDirectory, String outputPath) {
        super("health-checker",
                Versions.HEALTH_CHECKER,
                "health-checker.jar",
                "10G",
                Lists.newArrayList("-run_dir", runDirectory, "-report_file_path", outputPath));
    }
}
