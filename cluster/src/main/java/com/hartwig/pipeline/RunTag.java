package com.hartwig.pipeline;

public class RunTag {

    public static String apply(Arguments arguments, String id) {
        return arguments.runId().map(runId -> maybeWithShallow(arguments, id) + "-" + runId).orElse(maybeWithShallow(arguments, id));
    }

    private static String maybeWithShallow(final Arguments arguments, final String id) {
        return arguments.shallow() ? id + "-shallow" : id;
    }
}
