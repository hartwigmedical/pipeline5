package com.hartwig.bcl2fastq.qc;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableStats.class)
@Value.Style(jdkOnly=true)
public interface Stats {

    String flowcell();

    List<LaneStats> conversionResults();
}