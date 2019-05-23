package com.hartwig.pipeline.tertiary.healthcheck;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public class HealthCheckEvaluationCommand implements BashCommand {

    private final String outputPath;
    private final String healthCheckFile;

    HealthCheckEvaluationCommand(final String outputPath, final String healthCheckFile) {
        this.outputPath = outputPath;
        this.healthCheckFile = healthCheckFile;
    }

    @Override
    public String asBash() {
        return String.format("if [[ $(perl %s/health-checker/%s/do_healthcheck_qctests.pl  --healthcheck-log-file %s | tail -1 ) =~ OK ]];"
                        + " then touch \"%s/HealthCheckEvaluation.success\"; fi",
                VmDirectories.TOOLS,
                Versions.HEALTH_CHECKER,outputPath +  healthCheckFile,
                outputPath);
    }
}