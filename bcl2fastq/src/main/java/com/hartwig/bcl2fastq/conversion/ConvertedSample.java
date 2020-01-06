package com.hartwig.bcl2fastq.conversion;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedSample extends WithYieldAndQ30 {

    String barcode();

    String sample();

    String project();

    List<ConvertedFastq> fastq();

    static ImmutableConvertedSample.Builder builder() {
        return ImmutableConvertedSample.builder();
    }
}
