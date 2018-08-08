package com.hartwig.patient;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface Sample extends FileSystemEntity, Named {

    List<Lane> lanes();

    static ImmutableSample.Builder builder(final String directory, final String name) {
        return ImmutableSample.builder().directory(directory).name(name);
    }
}
