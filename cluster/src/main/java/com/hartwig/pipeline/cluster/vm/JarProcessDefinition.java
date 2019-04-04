package com.hartwig.pipeline.cluster.vm;

import org.immutables.value.Value;

@Value.Immutable
public interface JarProcessDefinition {
    String name();

    String jar();

    String bucket();
}
