package com.hartwig.io;

import com.hartwig.patient.FileSystemEntity;

public interface DataSource<E extends FileSystemEntity, P> {
    InputOutput<E, P> extract(E entity);
}
