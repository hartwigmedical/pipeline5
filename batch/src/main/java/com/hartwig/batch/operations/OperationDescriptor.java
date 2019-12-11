package com.hartwig.batch.operations;

import org.immutables.value.Value;

@Value.Immutable
public interface OperationDescriptor {
    String callName();

    String description();

    static OperationDescriptor of(String callName, String description) {
        return ImmutableOperationDescriptor.builder().callName(callName).description(description).build();
    }
}
