package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.function.Function;

public class FileStreamSupplier implements Function<File, InputStream> {
    @Override
    public InputStream apply(final File file) {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Function<File, InputStream> newInstance() {
        return new FileStreamSupplier();
    }
}
