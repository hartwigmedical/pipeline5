package com.hartwig.bcl2fastq.conversion;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedFastq extends WithYieldAndQ30{

    FastqId id();

    String pathR1();

    Optional<String> pathR2();

    String outputPathR1();

    Optional<String> outputPathR2();

    long sizeR1();

    Optional<Long> sizeR2();

    String md5R1();

    Optional<String> md5R2();

    static ImmutableConvertedFastq.Builder builder() {
        return ImmutableConvertedFastq.builder();
    }
}