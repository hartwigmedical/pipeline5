package com.hartwig.patient;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface KnownSnps {

    @Value.Parameter
    List<String> paths();

    static KnownSnps of(List<String> paths) {
        return ImmutableKnownSnps.of(paths);
    }
}
