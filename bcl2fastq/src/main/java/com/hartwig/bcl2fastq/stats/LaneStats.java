package com.hartwig.bcl2fastq.stats;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableLaneStats.class)
@Value.Style(jdkOnly = true)
public interface LaneStats {

    int laneNumber();

    List<SampleStats> demuxResults();

    @Value.Default
    default UndeterminedStats undetermined() {
        return ImmutableUndeterminedStats.builder().yield(0).build();
    }
}
