package com.hartwig.pipeline.bootstrap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments, LocalDateTime now) {
        String id = arguments.runId().orElse(now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")));
        return ImmutableRun.of(String.format("run-%s-%s", sampleName.toLowerCase(), id));
    }
}
