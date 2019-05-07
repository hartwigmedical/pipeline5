package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;

class HealthCheckerApplicationCommand extends JavaJarCommand {

    HealthCheckerApplicationCommand(String runDirectory, String outputDirectory) {
        super("health-checker",
                "2.4",
                "health-checker",
                "10G",
                Lists.newArrayList("-run_dir", runDirectory, "-report_file_path", outputDirectory));
    }
}
