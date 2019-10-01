package com.hartwig.pipeline.tertiary.chord;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface ChordOutput extends StageOutput{

    @Override
    default String name() {
        return Chord.NAMESPACE;
    }

    static ImmutableChordOutput.Builder builder() {
        return ImmutableChordOutput.builder();
    }
}
