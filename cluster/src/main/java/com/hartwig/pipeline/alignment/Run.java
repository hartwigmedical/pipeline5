package com.hartwig.pipeline.alignment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.pipeline.Arguments;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments, LocalDateTime now) {
        String id;
        if (arguments.profile().equals(Arguments.DefaultsProfile.DEVELOPMENT)) {
            String suffix = arguments.runId().orElse(now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            id = String.format("%s-%s", sampleName.toLowerCase(), suffix);
        } else {
            id = sampleName.toLowerCase();
        }
        return ImmutableRun.of(String.format("run-%s", id));
    }
}
