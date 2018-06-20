package com.hartwig.io;

import com.hartwig.patient.FileSystemEntity;

public interface OutputStore<E extends FileSystemEntity, P> {

    void store(InputOutput<E, P> inputOutput);

    boolean exists(E entity, OutputType type);
}
