package com.hartwig.bcl2fastq.conversion;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface Conversion {

    String flowcell();

    long undeterminedReads();

    long totalReads();

    List<ConvertedSample> samples();

    static ImmutableConversion.Builder builder(){
       return ImmutableConversion.builder();
    }
}
