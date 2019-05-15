package com.hartwig.pipeline.tertiary.healthcheck;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public class HealthCheckEvaluationCommand implements BashCommand {

    private final String runDirectory;

    HealthCheckEvaluationCommand(final String runDirectory) {
        this.runDirectory = runDirectory;
    }

    @Override
    public String asBash() {
        return String.format("perl %s/health-checker/%s/do_healthcheck_qctests.pl  --healthcheck-log-file %s/HealthCheck.out",
                VmDirectories.TOOLS,
                Versions.HEALTH_CHECKER,
                runDirectory);
    }
}
