package com.hartwig.pipeline.storage;

import java.util.function.Function;

public class GSFileSource implements Function<String, String> {

    @Override
    public String apply(final String file) {
        return String.format("gs://%s", file);
    }
}
