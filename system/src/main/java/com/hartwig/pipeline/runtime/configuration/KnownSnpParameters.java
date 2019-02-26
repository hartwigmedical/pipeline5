package com.hartwig.pipeline.runtime.configuration;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableKnownSnpParameters.class)
@Value.Immutable
@ParameterStyle
public interface KnownSnpParameters {

    @Value.Default
    default String directory() {
        return "/known_snps";
    }

    List<String> files();

    default List<String> paths() {
        return files().stream().map(file -> directory() + File.separator + file).collect(Collectors.toList());
    }

    static ImmutableKnownSnpParameters.Builder builder() {
        return ImmutableKnownSnpParameters.builder();
    }
}
