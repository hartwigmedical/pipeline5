package com.hartwig.pipeline.io;

import java.util.function.Function;

import com.hartwig.patient.Sample;

public class LocalFileTarget implements Function<Sample, String> {

    @Override
    public String apply(final Sample sample) {
        return String.format("%s/%s.bam", System.getProperty("user.dir"), sample.name());
    }
}
