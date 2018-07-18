package com.hartwig.pipeline.runtime.configuration;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableConfiguration.class)
@Value.Immutable
public interface Configuration {

    enum Flavour {
        GATK,
        ADAM
    }

    PipelineParameters pipeline();

    PatientParameters patient();

    ReferenceGenomeParameters referenceGenome();

    KnownIndelParameters knownIndel();

    Map<String, String> spark();
}
