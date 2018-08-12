package com.hartwig.pipeline.runtime.configuration;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableConfiguration.class)
@Value.Immutable
public interface Configuration {

    PipelineParameters pipeline();

    @Value.Default
    default PatientParameters patient() {
        return ImmutablePatientParameters.builder().build();
    }

    ReferenceGenomeParameters referenceGenome();

    KnownIndelParameters knownIndel();

    Map<String, String> spark();
}
