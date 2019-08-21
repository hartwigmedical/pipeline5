package com.hartwig.pipeline.labels;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.tools.Versions;

public class Labels {

    public static Map<String, String> ofRun(String run, String jobName, Arguments arguments) {
        return ImmutableMap.of("run_id",
                run,
                "job_name",
                jobName.toLowerCase(),
                "version",
                Versions.pipelineMajorMinorVersion(),
                "shallow",
                arguments.shallow() ? "true" : "false");
    }
}
