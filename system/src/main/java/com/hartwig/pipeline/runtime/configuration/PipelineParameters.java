package com.hartwig.pipeline.runtime.configuration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePipelineParameters.class)
@Value.Immutable
public interface PipelineParameters {

    Configuration.Flavour flavour();
}
