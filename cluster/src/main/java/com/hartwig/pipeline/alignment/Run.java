package com.hartwig.pipeline.alignment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.pipeline.Arguments;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments) {
        return ImmutableRun.of(String.format("run-%s",
                arguments.runId().map(id -> String.format("%s-%s", sampleName.toLowerCase(), id)).orElse(sampleName.toLowerCase())));
    }
}
