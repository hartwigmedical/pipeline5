package com.hartwig.pipeline.labels;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.tools.Versions;

public class Labels {

    public static Map<String, String> ofRun(String run, String jobName, Arguments arguments) {
        return ImmutableMap.of("run_id", clean(run),
                "job_name", clean(jobName),
                "version", clean(Versions.pipelineMajorMinorVersion()),
                "shallow",
                arguments.shallow() ? "true" : "false");
    }

    private static String clean(final String run) {
        return run.toLowerCase().replace("_", "-").replace("\\.", "-");
    }
}
