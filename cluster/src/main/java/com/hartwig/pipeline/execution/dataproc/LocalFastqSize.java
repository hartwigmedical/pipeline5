package com.hartwig.pipeline.execution.dataproc;

import java.io.File;
import java.util.function.ToDoubleFunction;

public class LocalFastqSize implements ToDoubleFunction<String> {
    @Override
    public double applyAsDouble(final String value) {
        return new File(value.replaceAll("file:", "")).length() / 1e9;
    }
}
