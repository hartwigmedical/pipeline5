package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;

public interface Stage<I, O> {

    DataSource<I> datasource();

    OutputType outputType();

    InputOutput<O> execute(InputOutput<I> input) throws IOException;
}
