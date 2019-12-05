package com.hartwig.bcl2fastq.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSbpSample.class)
@JsonDeserialize(as = ImmutableSbpSample.class)
public interface SbpSample {

    Optional<String> id();

    String barcode();

    String submission();

    String status();

    static ImmutableSbpSample.Builder builder() {
        return ImmutableSbpSample.builder();
    }
}
