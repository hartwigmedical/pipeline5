package com.hartwig.pipeline.bootstrap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.hartwig.patient.Sample;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(Sample sample, Arguments arguments, LocalDateTime now) {
        String id = arguments.runId().orElse(now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")));
        return ImmutableRun.of(String.format("run-%s-%s", sample.name().toLowerCase(), id));
    }
}
