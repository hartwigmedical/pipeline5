package com.hartwig.bcl2fastq.conversion;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedUndetermined extends WithYieldAndQ30 {

    static ImmutableConvertedUndetermined.Builder builder() {
        return ImmutableConvertedUndetermined.builder();
    }
}
