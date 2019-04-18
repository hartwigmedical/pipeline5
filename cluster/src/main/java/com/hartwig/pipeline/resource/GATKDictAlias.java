package com.hartwig.pipeline.resource;

import java.util.function.Function;

public class GATKDictAlias implements Function<String, String> {

    private static final String DICT_EXTENSION = ".dict";

    @Override
    public String apply(final String file) {
        if (file.endsWith(DICT_EXTENSION)) {
            String[] splitFile = file.split("\\.");
            return splitFile[0] + DICT_EXTENSION;
        }
        return file;
    }
}
