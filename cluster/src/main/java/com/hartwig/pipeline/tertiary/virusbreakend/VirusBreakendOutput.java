package com.hartwig.pipeline.tertiary.virusbreakend;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusBreakendOutput extends StageOutput {

    @Override
    default String name() {
        return VirusBreakend.NAMESPACE;
    }

    Optional<VirusBreakendOutputLocations> maybeOutputLocations();

    default VirusBreakendOutputLocations outputLocations() {
        return maybeOutputLocations().orElseThrow();
    }

    static ImmutableVirusBreakendOutput.Builder builder() {
        return ImmutableVirusBreakendOutput.builder();
    }
}
