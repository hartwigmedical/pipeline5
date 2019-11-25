package com.hartwig.bcl2fastq.qc;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableDemuxedStats.class)
public interface DemuxedStats {

    String sampleId();

    String yield();
}
