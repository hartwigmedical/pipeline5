package com.hartwig.bcl2fastq.qc;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSampleStats.class)
public interface SampleStats {

    Optional<String> sampleId();

    long yield();
}
