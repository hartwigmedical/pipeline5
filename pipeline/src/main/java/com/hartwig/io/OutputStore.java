package com.hartwig.io;

import com.hartwig.patient.FileSystemEntity;

public interface OutputStore<E extends FileSystemEntity, P> {

    void store(Output<E, P> output);
}
