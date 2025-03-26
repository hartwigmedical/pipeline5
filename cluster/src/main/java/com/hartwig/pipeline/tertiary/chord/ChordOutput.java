package com.hartwig.pipeline.tertiary.chord;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface ChordOutput extends StageOutput {
    static ImmutableChordOutput.Builder builder() {
        return ImmutableChordOutput.builder();
    }

    @Override
    default String name() {
        return Chord.NAMESPACE;
    }

    Optional<ChordOutputLocations> maybeChordOutputLocations();

    default ChordOutputLocations chordOutputLocations() {
        return maybeChordOutputLocations().orElse(ChordOutputLocations.builder()
                .predictions(GoogleStorageLocation.empty())
                .signatures(GoogleStorageLocation.empty())
                .build());
    }
}
