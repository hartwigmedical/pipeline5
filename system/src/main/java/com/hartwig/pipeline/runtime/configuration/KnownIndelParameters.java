package com.hartwig.pipeline.runtime.configuration;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableKnownIndelParameters.class)
@Value.Immutable
@ParameterStyle
public interface KnownIndelParameters {

    @Value.Default
    default String directory() {
        return "/known_indels";
    }

    List<String> files();

    default List<String> paths() {
        return files().stream().map(file -> directory() + File.separator + file).collect(Collectors.toList());
    }

    @Value.Check
    default void validate() {
        if (files().isEmpty()) {
            throw new IllegalStateException("Pipeline.yaml must contain at least one known indel vcf");
        }
    }

    static ImmutableKnownIndelParameters.Builder builder() {
        return ImmutableKnownIndelParameters.builder();
    }
}
