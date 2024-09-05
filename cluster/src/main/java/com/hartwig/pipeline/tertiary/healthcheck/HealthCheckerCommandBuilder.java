package com.hartwig.pipeline.tertiary.healthcheck;

import static com.hartwig.pipeline.tools.HmfTool.HEALTH_CHECKER;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.JavaCommandFactory;

class HealthCheckerCommandBuilder
{

    private final List<String> arguments;

    HealthCheckerCommandBuilder(final String purplePath, final String outputPath) {
        arguments = Lists.newArrayList("-purple_dir", purplePath, "-output_dir", outputPath);
    }

    HealthCheckerCommandBuilder withTumor(final String tumorSampleName, final String tumorMetricsPath,
            final String tumorFlagstatPath) {
        arguments.addAll(List.of("-tumor",
                tumorSampleName,
                "-tum_wgs_metrics_file",
                tumorMetricsPath,
                "-tum_flagstat_file",
                tumorFlagstatPath));
        return this;
    }

    HealthCheckerCommandBuilder withReference(final String refSampleName, final String refMetricsPath,
            final String refFlagstatPath) {
        arguments.addAll(List.of("-reference",
                refSampleName,
                "-ref_wgs_metrics_file",
                refMetricsPath,
                "-ref_flagstat_file",
                refFlagstatPath));
        return this;
    }

    BashCommand build() {
        return JavaCommandFactory.javaJarCommand(HEALTH_CHECKER, arguments);
    }
}
