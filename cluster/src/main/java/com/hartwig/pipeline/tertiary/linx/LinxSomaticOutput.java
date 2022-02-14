package com.hartwig.pipeline.tertiary.linx;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxSomaticOutput extends StageOutput {

    @Override
    default String name() {
        return LinxSomatic.NAMESPACE;
    }

    static ImmutableLinxSomaticOutput.Builder builder() {
        return ImmutableLinxSomaticOutput.builder();
    }

    Optional<LinxSomaticOutputLocations> maybeLinxOutputLocations();

    default LinxSomaticOutputLocations linxOutputLocations() {
        return maybeLinxOutputLocations().orElseThrow();
    }
}
