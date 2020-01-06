package com.hartwig.bcl2fastq.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSbpFastq.class)
@JsonDeserialize(as = ImmutableSbpFastq.class)
public interface SbpFastq {

    Optional<Integer> id();

    int sample_id();

    int lane_id();

    boolean qc_pass();

    Optional<Double> q30();

    Optional<Long> yld();

    String bucket();

    String name_r1();

    String name_r2();

    Optional<String> hash_r1();

    Optional<String> hash_r2();

    Optional<Long> size_r1();

    Optional<Long> size_r2();

    static ImmutableSbpFastq.Builder builder() {
        return ImmutableSbpFastq.builder();
    }
}
