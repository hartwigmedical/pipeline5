package com.hartwig.pipeline.alignment;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentPair {

    AlignmentOutput reference();

    AlignmentOutput tumor();

    static AlignmentPair of(AlignmentOutput alignmentOutput, AlignmentOutput complement) {
        return null;
    }
}
