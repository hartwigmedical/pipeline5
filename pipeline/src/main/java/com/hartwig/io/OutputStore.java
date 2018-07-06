package com.hartwig.io;

import java.io.File;

import com.hartwig.patient.FileSystemEntity;

public interface OutputStore<E extends FileSystemEntity, P> {

    void store(InputOutput<E, P> inputOutput);

    default boolean exists(final E entity, final OutputType type) {
        return new File(OutputFile.of(type, entity).path()).exists();
    }
}
