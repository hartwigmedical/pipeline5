package com.hartwig.bcl2fastq.stats;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableUndeterminedStats.class)
public interface UndeterminedStats {

    long yield();
}
