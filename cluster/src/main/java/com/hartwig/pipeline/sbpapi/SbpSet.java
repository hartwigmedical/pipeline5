package com.hartwig.pipeline.sbpapi;

import java.time.LocalDateTime;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpSet.class)
@Value.Immutable
public interface SbpSet {

    LocalDateTime createTime();

    String name();

    String id();
}