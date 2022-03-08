package com.hartwig.pipeline.execution.vm;

import org.immutables.value.Value;

@Value.Immutable
public interface RuntimeFiles {
    String failure();

    String success();

    String log();

    String startupScript();

    static ImmutableRuntimeFiles of(final String prefix) {
        String flagLabel = prefix.isEmpty() ? "JOB" : prefix;
        String logLabel = prefix.isEmpty() ? "" : prefix + "_";
        return ImmutableRuntimeFiles.builder()
                .success(flagLabel + "_SUCCESS")
                .failure(flagLabel + "_FAILURE")
                .log(logLabel + "run.log")
                .startupScript(logLabel + "copy_of_startup_script_for_run.sh")
                .build();
    }

    static ImmutableRuntimeFiles typical() {
        return of("");
    }
}
