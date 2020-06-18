package com.hartwig.pipeline.turquoise;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSubject.class)
interface Subject {

    @Value.Parameter
    String name();

    @Value.Parameter
    SubjectType type();

    static Subject of(String name, String type) {
        return ImmutableSubject.of(name, ImmutableSubjectType.of(type));
    }
}
