package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments) {
        return ImmutableRun.of(format("run-%s", RunTag.apply(arguments, sampleName.toLowerCase())));
    }

    static Run from(String reference, String tumor, Arguments arguments) {
        return from(format("%s-%s", reference, tumor), arguments);
    }
}