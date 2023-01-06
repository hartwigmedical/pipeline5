package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.input.RunMetadata;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(final RunMetadata runMetadata, final CommonArguments arguments) {
        return ImmutableRun.of(format("run-%s", RunTag.apply(arguments, runMetadata.runName().toLowerCase())).replace("_", "-"));
    }
}