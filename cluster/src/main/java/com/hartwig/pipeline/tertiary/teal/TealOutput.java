package com.hartwig.pipeline.tertiary.teal;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface TealOutput extends StageOutput {

    @Override
    default String name() {
        return Teal.NAMESPACE;
    }

    Optional<TealOutputLocations> maybeOutputLocations();

    static ImmutableTealOutput.Builder builder() {
        return ImmutableTealOutput.builder();
    }
}
