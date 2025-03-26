package com.hartwig.pipeline.tertiary.vchord;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface VChordOutput extends StageOutput {

    @Override
    default String name() {
        return VChord.NAMESPACE;
    }

    Optional<VChordOutputLocations> maybeOutputLocations();

    static ImmutableVChordOutput.Builder builder() {
        return ImmutableVChordOutput.builder();
    }
}
