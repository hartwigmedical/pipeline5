package com.hartwig.bcl2fastq.stats;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSampleStats.class)
@Value.Style(jdkOnly=true)
public interface SampleStats {

    @JsonProperty(value = "sampleId")
    String barcode();

    long yield();

    List<ReadMetrics> readMetrics();
}
