package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.PipelineStatus;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface AlignmentPair {

    Optional<AlignmentOutput> maybeReference();

    Optional<AlignmentOutput> maybeTumor();

    default AlignmentOutput reference() {
        return maybeReference().orElse(AlignmentOutput.builder().sample("empty").status(PipelineStatus.SKIPPED).build());
    }

    default AlignmentOutput tumor() {
        return maybeTumor().orElse(AlignmentOutput.builder().sample("empty").status(PipelineStatus.SKIPPED).build());
    }

    static ImmutableAlignmentPair.Builder builder() {
        return ImmutableAlignmentPair.builder();
    }

    static AlignmentPair of(final AlignmentOutput referenceAlignmentOutput, final AlignmentOutput tumorAlignmentOutput) {
        return builder().maybeReference(referenceAlignmentOutput).maybeTumor(tumorAlignmentOutput).build();
    }
}
