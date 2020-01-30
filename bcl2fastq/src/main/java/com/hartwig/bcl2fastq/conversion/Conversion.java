package com.hartwig.bcl2fastq.conversion;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface Conversion extends WithYieldAndQ30 {

    String flowcell();

    long undeterminedReads();

    long totalReads();

    @Value.Derived
    @Override
    default long yield() {
        return totalReads();
    }

    @Value.Derived
    default long yieldQ30() {
        return samples().stream().flatMap(s -> s.fastq().stream()).mapToLong(WithYieldAndQ30::yieldQ30).sum();
    }

    List<ConvertedSample> samples();

    static ImmutableConversion.Builder builder() {
        return ImmutableConversion.builder();
    }
}
