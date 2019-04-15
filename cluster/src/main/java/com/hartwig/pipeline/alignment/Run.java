package com.hartwig.pipeline.alignment;

import static java.lang.String.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.pipeline.Arguments;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments) {
        return ImmutableRun.of(format("run-%s",
                arguments.runId().map(id -> format("%s-%s", sampleName.toLowerCase(), id)).orElse(sampleName.toLowerCase())));
    }

    static Run from(String reference, String tumor, Arguments arguments) {
        return from(format("%s-%s", reference, tumor), arguments);
    }
}
