package com.hartwig.bcl2fastq.conversion;

import java.util.List;
import java.util.function.ToLongFunction;

import org.immutables.value.Value;

@Value.Immutable
public interface Conversion extends WithYieldAndQ30 {

    String flowcell();

    @Value.Derived
    @Override
    default long yield() {
        return sumDeterminedAndUndetermined(WithYieldAndQ30::yield);
    }

    @Value.Derived
    default long yieldQ30() {
        return sumDeterminedAndUndetermined(WithYieldAndQ30::yieldQ30);
    }

    ConvertedUndetermined undetermined();

    List<ConvertedSample> samples();

    static ImmutableConversion.Builder builder() {
        return ImmutableConversion.builder();
    }

    private long sumDeterminedAndUndetermined(final ToLongFunction<WithYieldAndQ30> mapToLong) {
        return samples().stream().flatMap(s -> s.fastq().stream()).mapToLong(mapToLong).sum() + mapToLong.applyAsLong(undetermined());
    }
}
