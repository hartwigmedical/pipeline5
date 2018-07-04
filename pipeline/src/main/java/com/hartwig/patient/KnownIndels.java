package com.hartwig.patient;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface KnownIndels {

    @Value.Parameter
    List<String> paths();

    static KnownIndels of(List<String> knownIndelPaths) {
        return ImmutableKnownIndels.of(knownIndelPaths);
    }
}
