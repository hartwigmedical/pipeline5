package com.hartwig.batch.operations;

import org.immutables.value.Value;

@Value.Immutable
public interface CommandDescriptor {
    String callName();

    String description();

    static CommandDescriptor of(String callName, String description) {
        return ImmutableCommandDescriptor.builder().callName(callName).description(description).build();
    }
}
