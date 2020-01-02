package com.hartwig.pipeline.labels;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.tools.Versions;

public class Labels {

    public static Map<String, String> ofRun(String run, String jobName) {
        return ImmutableMap.of("run_id",
                clean(run),
                "job_name",
                clean(jobName),
                "version",
                clean(Versions.pipelineMajorMinorVersion()));
    }

    private static String clean(final String run) {
        return run.toLowerCase().replace("_", "-").replace('.', '-');
    }
}
