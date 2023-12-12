package com.hartwig.pipeline.tertiary.chord;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface ChordOutput extends StageOutput {

    @Override
    default String name() {
        return Chord.NAMESPACE;
    }

    static ImmutableChordOutput.Builder builder() {
        return ImmutableChordOutput.builder();
    }

    Optional<GoogleStorageLocation> maybePredictions();

    default GoogleStorageLocation predictions() {
        return maybePredictions().orElse(GoogleStorageLocation.empty());
    }
}
