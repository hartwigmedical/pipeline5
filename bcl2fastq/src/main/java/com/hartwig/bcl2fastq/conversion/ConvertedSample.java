package com.hartwig.bcl2fastq.conversion;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface ConvertedSample extends WithYieldAndQ30 {

    String barcode();

    String sample();

    String project();

    List<ConvertedFastq> fastq();

    @Override
    @Value.Derived
    default long yield() {
        return fastq().stream().mapToLong(WithYieldAndQ30::yield).sum();
    }

    @Override
    @Value.Derived
    default long yieldQ30() {
        return fastq().stream().mapToLong(WithYieldAndQ30::yieldQ30).sum();
    }

    static ImmutableConvertedSample.Builder builder() {
        return ImmutableConvertedSample.builder();
    }
}
