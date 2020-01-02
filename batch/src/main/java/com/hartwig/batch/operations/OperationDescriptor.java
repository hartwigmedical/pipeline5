package com.hartwig.batch.operations;

import org.immutables.value.Value;

@Value.Immutable
public interface OperationDescriptor {
    enum InputType {
        FLAT, JSON
    }

    String callName();

    String description();

    InputType inputType();

    static OperationDescriptor of(String callName, String description, InputType inputType) {
        return ImmutableOperationDescriptor.builder().callName(callName).description(description)
                .inputType(inputType).build();
    }
}
