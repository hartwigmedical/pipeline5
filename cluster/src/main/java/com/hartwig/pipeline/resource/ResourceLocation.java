package com.hartwig.pipeline.resource;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface ResourceLocation {

    String bucket();

    List<String> files();

    static ImmutableResourceLocation.Builder builder() {
        return ImmutableResourceLocation.builder();
    }
}
