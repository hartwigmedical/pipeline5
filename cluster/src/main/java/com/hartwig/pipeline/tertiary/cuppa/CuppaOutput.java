package com.hartwig.pipeline.tertiary.cuppa;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutput extends StageOutput {
    static ImmutableCuppaOutput.Builder builder() {
        return ImmutableCuppaOutput.builder();
    }

    @Override
    default String name() {
        return Cuppa.NAMESPACE;
    }

    Optional<CuppaOutputLocations> maybeCuppaOutputLocations();

    default CuppaOutputLocations cuppaOutputLocations() {
        return maybeCuppaOutputLocations().orElseThrow();
    }
}
