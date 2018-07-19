package com.hartwig.io;

import java.util.Optional;

import com.hartwig.patient.FileSystemEntity;
import com.hartwig.patient.Sample;

import org.immutables.value.Value;

@Value.Immutable
public interface InputOutput<P> {

    String MISSING_OPTIONAL = "No [%s] in this I/O object. Perhaps this is the seed of the pipeline (ie not an output of another stage)?";

    @Value.Parameter
    Sample sample();

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

    static <E extends FileSystemEntity, P> InputOutput<P> seed(Sample sample) {
        return ImmutableInputOutput.of(sample, Optional.empty(), Optional.empty());
    }

    static <E extends FileSystemEntity, P> InputOutput<P> of(OutputType type, Sample sample, P payload) {
        return ImmutableInputOutput.of(sample, Optional.of(type), Optional.of(payload));
    }
}
