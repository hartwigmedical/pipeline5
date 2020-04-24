package com.hartwig.patient;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSample.class)
@Value.Style(jdkOnly = true)
public interface Sample {

    String NOT_APPLICABLE = "NA";

    enum Type {
        TUMOR,
        REFERENCE
    }

    String name();

    Type type();

    List<Lane> lanes();

    @Value.Default
    default String barcode() {
        return NOT_APPLICABLE;
    }

    static ImmutableSample.Builder builder(final String name) {
        return ImmutableSample.builder().name(name);
    }
}
