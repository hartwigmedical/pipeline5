package com.hartwig.pipeline.tertiary.cider;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface CiderOutput extends StageOutput {

    @Override
    default String name() {
        return Cider.NAMESPACE;
    }

    Optional<CiderOutputLocations> maybeOutputLocations();

    static ImmutableCiderOutput.Builder builder() {
        return ImmutableCiderOutput.builder();
    }
}
