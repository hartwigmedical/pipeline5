package com.hartwig.pipeline.runtime.configuration;

import java.io.File;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableReferenceGenomeParameters.class)
@Value.Immutable
@ParameterStyle
public interface ReferenceGenomeParameters {

    @Value.Default
    default String directory() {
        return "/reference_genome";
    }

    String file();

    default String path() {
        return directory() + File.separator + file();
    }

    static ImmutableReferenceGenomeParameters.Builder builder() {
        return ImmutableReferenceGenomeParameters.builder();
    }
}
