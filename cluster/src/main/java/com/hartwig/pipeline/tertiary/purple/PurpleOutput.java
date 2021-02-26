package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutput extends StageOutput {

    @Override
    default String name() {
        return Purple.NAMESPACE;
    }

    Optional<PurpleOutputLocations> maybeOutputLocations();

    default PurpleOutputLocations outputLocations() {
        return maybeOutputLocations().orElseThrow();
    }

    static ImmutablePurpleOutput.Builder builder() {
        return ImmutablePurpleOutput.builder();
    }
}
