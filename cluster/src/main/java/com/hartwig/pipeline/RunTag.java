package com.hartwig.pipeline;

public class RunTag {

    public static String apply(final CommonArguments arguments, final String setName) {
        return arguments.runTag()
                .map(runId -> setName + "-" + runId)
                .orElse(setName + arguments.sbpApiRunId().map(String::valueOf).map(str -> "-" + str).orElse(""));
    }
}
