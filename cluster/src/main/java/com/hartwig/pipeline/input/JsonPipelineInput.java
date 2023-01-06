package com.hartwig.pipeline.input;

import java.io.File;
import java.io.IOException;

import com.hartwig.pipeline.jackson.ObjectMappers;

public class JsonPipelineInput {

    public static PipelineInput read(final String filename) {
        try {
            return ObjectMappers.get().readValue(new File(filename), PipelineInput.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
