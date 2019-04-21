package com.hartwig.pipeline.resource;

import static java.util.stream.Stream.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.immutables.value.Value;

@Value.Immutable
public interface ResourceLocation {

    String bucket();

    List<String> files();

    static ImmutableResourceLocation.Builder builder() {
        return ImmutableResourceLocation.builder();
    }
}
