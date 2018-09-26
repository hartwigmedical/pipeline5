package com.hartwig.pipeline.upload;

import java.util.function.Function;

public class LocalFileLocation implements Function<String, String> {

    @Override
    public String apply(final String s) {
        return s.replace("file:", "");
    }
}
