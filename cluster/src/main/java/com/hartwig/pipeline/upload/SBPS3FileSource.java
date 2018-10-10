package com.hartwig.pipeline.upload;

import java.util.function.Function;

public class SBPS3FileSource implements Function<String, String> {

    @Override
    public String apply(final String file) {
        return String.format("s3://%s", file);
    }
}
