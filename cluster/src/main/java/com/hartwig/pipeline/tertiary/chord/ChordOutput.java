package com.hartwig.pipeline.tertiary.chord;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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
