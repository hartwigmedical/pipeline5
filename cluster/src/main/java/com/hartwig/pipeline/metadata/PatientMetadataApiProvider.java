package com.hartwig.pipeline.metadata;

import java.time.LocalDateTime;

import com.hartwig.pipeline.Arguments;

public class PatientMetadataApiProvider {

    private final Arguments arguments;

    private PatientMetadataApiProvider(final Arguments arguments) {
        this.arguments = arguments;
    }

    public PatientMetadataApi get() {
        return new PatientMetadataApi(arguments, LocalDateTime.now());
    }

    public static PatientMetadataApiProvider from(final Arguments arguments) {
        return new PatientMetadataApiProvider(arguments);
    }
}