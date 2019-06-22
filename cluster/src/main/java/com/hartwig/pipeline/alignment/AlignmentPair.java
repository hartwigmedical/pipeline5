package com.hartwig.pipeline.alignment;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentPair {

    @Value.Parameter
    AlignmentOutput reference();

    @Value.Parameter
    AlignmentOutput tumor();

    static AlignmentPair of(AlignmentOutput reference, AlignmentOutput tumor) {
        return ImmutableAlignmentPair.of(reference, tumor);
    }
}
