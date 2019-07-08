package com.hartwig.pipeline.storage;

import java.util.function.Function;

public class LocalFileSource implements Function<String, String> {

    @Override
    public String apply(final String s) {
        return s.replace("file:", "");
    }
}
