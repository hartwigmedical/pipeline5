package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.metadata.RunMetadata;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(RunMetadata runMetadata, Arguments arguments) {
        return ImmutableRun.of(format("run-%s", RunTag.apply(arguments, runMetadata.name().toLowerCase())).replace("_", "-"));
    }
}