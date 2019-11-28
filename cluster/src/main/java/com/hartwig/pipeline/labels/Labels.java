package com.hartwig.pipeline.labels;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.tools.Versions;

import java.util.Map;

public class Labels {

    public static Map<String, String> ofRun(String run, String jobName, CommonArguments arguments) {
        return ImmutableMap.of("run_id",
                run.toLowerCase(),
                "job_name",
                jobName.toLowerCase(),
                "version",
                Versions.pipelineMajorMinorVersion().toLowerCase(),
                "shallow",
                arguments.shallow() ? "true" : "false");
    }
}
