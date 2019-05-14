package com.hartwig.pipeline.alignment;

import java.util.stream.Stream;

import com.hartwig.patient.Sample;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface AlignmentPair {

    @Value.Parameter
    AlignmentOutput reference();

    @Value.Parameter
    AlignmentOutput tumor();

    static AlignmentPair of(AlignmentOutput first, AlignmentOutput second) {

        return ImmutableAlignmentPair.of(find(first, second, Sample.Type.REFERENCE), find(first, second, Sample.Type.TUMOR));
    }

    @NotNull
    static AlignmentOutput find(final AlignmentOutput first, final AlignmentOutput second, final Sample.Type type) {
        return Stream.of(first, second)
                .filter(output -> output.sample().type().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "No [%s] sample in alignment pair. Check the set for misconfiguration or duplicate samples.",
                        type)));
    }
}
