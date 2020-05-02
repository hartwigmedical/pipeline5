package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableAddFileApiResponse.class)
public interface AddFileApiResponse {
    int id();
}
