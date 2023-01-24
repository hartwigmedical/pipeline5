package com.hartwig.pipeline.storage;

import java.util.function.Function;

import com.hartwig.pipeline.input.Sample;

public class LocalFileTarget implements Function<Sample, String> {

    @Override
    public String apply(final Sample sample) {
        return String.format("%s/%s.bam", System.getProperty("user.dir"), sample.name());
    }
}
