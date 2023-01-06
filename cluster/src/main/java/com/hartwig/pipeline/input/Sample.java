package com.hartwig.pipeline.input;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSample.class)
@Value.Style(jdkOnly = true)
public interface Sample {

    String NOT_APPLICABLE = "NA";

    String name();

    List<Lane> lanes();

    Optional<String> bam();

    List<String> primaryTumorDoids();

    @Value.Default
    default String barcode() {
        return NOT_APPLICABLE;
    }

    static ImmutableSample.Builder builder(final String name) {
        return ImmutableSample.builder().name(name);
    }
}