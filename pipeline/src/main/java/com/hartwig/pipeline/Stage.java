package com.hartwig.pipeline;

import java.io.IOException;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.FileSystemEntity;

public interface Stage<T extends FileSystemEntity, P> {

    OutputType outputType();

    InputOutput<T, P> execute(InputOutput<T, P> inputFromPreviousStage) throws IOException;
}
