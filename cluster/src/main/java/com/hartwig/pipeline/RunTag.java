package com.hartwig.pipeline;

public class RunTag {

    public static String apply(final CommonArguments arguments, final String id) {
        return arguments.runId()
                .map(runId -> id + "-" + runId)
                .orElse(id + arguments.sbpApiRunId().map(String::valueOf).map(str -> "-" + str).orElse(""));
    }
}
