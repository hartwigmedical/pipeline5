package com.hartwig.io;

import java.util.Optional;

import com.hartwig.patient.FileSystemEntity;

import org.immutables.value.Value;

@Value.Immutable
public interface InputOutput<E extends FileSystemEntity, P> {

    String MISSING_OPTIONAL = "No [%s] in this I/O object. Perhaps this is the seed of the pipeline (ie not an output of another stage)?";

    @Value.Parameter
    E entity();

    @Value.Parameter
    Optional<OutputType> maybeType();

    @Value.Parameter
    Optional<P> maybePayload();

    default OutputType type() {
        return maybeType().orElseThrow(() -> new IllegalStateException(String.format(MISSING_OPTIONAL, "output type")));
    }

    default P payload() {
        return maybePayload().orElseThrow(() -> new IllegalStateException(String.format(MISSING_OPTIONAL, "payload")));
    }

    static <E extends FileSystemEntity, P> InputOutput<E, P> seed(E entity) {
        return ImmutableInputOutput.of(entity, Optional.empty(), Optional.empty());
    }

    static <E extends FileSystemEntity, P> InputOutput<E, P> of(OutputType type, E entity, P payload) {
        return ImmutableInputOutput.of(entity, Optional.of(type), Optional.of(payload));
    }
}
