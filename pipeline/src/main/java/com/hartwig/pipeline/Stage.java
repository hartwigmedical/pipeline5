package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.FileSystemEntity;

public interface Stage<E extends FileSystemEntity, P> {

    DataSource<E, P> datasource();

    OutputType outputType();

    InputOutput<E, P> execute(InputOutput<E, P> input) throws IOException;
}
