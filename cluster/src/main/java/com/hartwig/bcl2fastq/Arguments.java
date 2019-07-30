package com.hartwig.bcl2fastq;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    String inputBucket();

    String flowcell();

    static ImmutableArguments.Builder builder(){
        return ImmutableArguments.builder();
    }
}
