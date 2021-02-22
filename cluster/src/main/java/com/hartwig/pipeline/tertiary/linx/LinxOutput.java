package com.hartwig.pipeline.tertiary.linx;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxOutput extends StageOutput {

    @Override
    default String name() {
        return Linx.NAMESPACE;
    }

    static ImmutableLinxOutput.Builder builder() {
        return ImmutableLinxOutput.builder();
    }

    Optional<LinxOutputLocations> maybeLinxOutputLocations();

    default LinxOutputLocations linxOutputLocations() {
        return maybeLinxOutputLocations().orElseThrow();
    }
}
