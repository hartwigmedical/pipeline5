package com.hartwig.bcl2fastq.stats;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableReadMetrics.class)
public interface ReadMetrics {

    int readNumber();

    long yield();

    long yieldQ30();
}
