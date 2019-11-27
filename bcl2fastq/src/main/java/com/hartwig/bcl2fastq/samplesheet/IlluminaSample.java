package com.hartwig.bcl2fastq.samplesheet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableIlluminaSample.class)
public interface IlluminaSample {

    @JsonProperty(value = "Sample_Project")
    String project();
}
