package com.hartwig.bcl2fastq.stats;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSampleStats.class)
@Value.Style(jdkOnly=true)
public interface SampleStats {

    String sampleId();

    long yield();

    List<ReadMetrics> readMetrics();
}
