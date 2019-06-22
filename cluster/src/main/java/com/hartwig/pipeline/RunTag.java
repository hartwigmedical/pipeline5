package com.hartwig.pipeline;

public class RunTag {

    public static String apply(Arguments arguments, String id){
        return arguments.runId().map(runId -> id + "-" + runId).orElse(id);
    }
}
