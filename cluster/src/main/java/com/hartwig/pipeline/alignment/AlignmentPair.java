package com.hartwig.pipeline.alignment;

import com.hartwig.computeengine.execution.ComputeEngineStatus;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface AlignmentPair {

    Optional<AlignmentOutput> maybeReference();

    Optional<AlignmentOutput> maybeTumor();

    default AlignmentOutput reference() {
        return maybeReference().orElse(AlignmentOutput.builder().sample("empty").status(ComputeEngineStatus.SKIPPED).build());
    }

    default AlignmentOutput tumor() {
        return maybeTumor().orElse(AlignmentOutput.builder().sample("empty").status(ComputeEngineStatus.SKIPPED).build());
    }

    static ImmutableAlignmentPair.Builder builder() {
        return ImmutableAlignmentPair.builder();
    }

    static AlignmentPair of(final AlignmentOutput referenceAlignmentOutput, final AlignmentOutput tumorAlignmentOutput) {
        return builder().maybeReference(referenceAlignmentOutput).maybeTumor(tumorAlignmentOutput).build();
    }
}
