package com.hartwig.pipeline.alignment;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentPair {

    Optional<AlignmentOutput> maybeReference();

    Optional<AlignmentOutput> maybeTumor();

    default AlignmentOutput reference() {
        return maybeReference().orElseThrow();
    }

    default AlignmentOutput tumor() {
        return maybeTumor().orElseThrow();
    }

    static ImmutableAlignmentPair.Builder builder() {
        return ImmutableAlignmentPair.builder();
    }

    static AlignmentPair of(AlignmentOutput referenceAlignmentOutput, AlignmentOutput tumorAlignmentOutput) {
        return builder().maybeReference(referenceAlignmentOutput).maybeTumor(tumorAlignmentOutput).build();
    }
}
