package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.FileSystemEntity;

public interface Stage<E extends FileSystemEntity, I, O> {

    DataSource<E, I> datasource();

    OutputType outputType();

    InputOutput<E, O> execute(InputOutput<E, I> input) throws IOException;
}
