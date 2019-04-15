package com.hartwig.pipeline.execution.dataproc;

import org.immutables.value.Value;

@Value.Immutable
public interface JarLocation {

    @Value.Parameter
    String uri();

    static JarLocation of(String uri) {
        return ImmutableJarLocation.of(uri);
    }
}
