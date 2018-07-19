package com.hartwig.io;

import java.io.File;

import com.hartwig.patient.Sample;

public interface OutputStore<P> {

    void store(InputOutput<P> inputOutput);

    default boolean exists(final Sample sample, final OutputType type) {
        return new File(OutputFile.of(type, sample).path()).exists();
    }
}
