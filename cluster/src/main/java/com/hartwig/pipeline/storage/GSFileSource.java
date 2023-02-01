package com.hartwig.pipeline.storage;

import java.util.function.Function;

public class GSFileSource implements Function<String, String> {

    private static final String GS = "gs://";

    @Override
    public String apply(final String file) {
        return file.startsWith(GS) ? file : String.format("%s%s", GS, file);
    }
}
