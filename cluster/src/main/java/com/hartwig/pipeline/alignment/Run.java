package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    int MAX_SAMPLE_LENGTH = 13;

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments) {
        return ImmutableRun.of(format("run-%s", RunTag.apply(arguments, sampleName.toLowerCase())).replace("_", "-"));
    }

    static Run from(String reference, String tumor, Arguments arguments) {
        return from(format("%s-%s", truncate(reference), truncate(tumor)), arguments);
    }

    static String truncate(final String sample) {
        return sample.length() > MAX_SAMPLE_LENGTH ? sample.substring(0, MAX_SAMPLE_LENGTH) : sample;
    }
}