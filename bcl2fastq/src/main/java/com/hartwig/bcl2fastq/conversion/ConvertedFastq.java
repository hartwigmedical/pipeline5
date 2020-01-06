package com.hartwig.bcl2fastq.conversion;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedFastq extends WithYieldAndQ30{

    FastqId id();

    String pathR1();

    String pathR2();

    String outputPathR1();

    String outputPathR2();

    long sizeR1();

    long sizeR2();

    String md5R1();

    String md5R2();

    static ImmutableConvertedFastq.Builder builder() {
        return ImmutableConvertedFastq.builder();
    }
}