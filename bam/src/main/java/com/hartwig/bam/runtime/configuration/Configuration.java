package com.hartwig.bam.runtime.configuration;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableConfiguration.class)
@Value.Immutable
@ParameterStyle
public interface Configuration {

    PipelineParameters pipeline();

    @Value.Default
    default PatientParameters patient() {
        return ImmutablePatientParameters.builder().build();
    }

    ReferenceGenomeParameters referenceGenome();

    Map<String, String> spark();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
