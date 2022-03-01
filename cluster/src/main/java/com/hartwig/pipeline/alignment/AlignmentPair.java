package com.hartwig.pipeline.alignment;

import java.util.Optional;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

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

    static AlignmentPair of(AlignmentOutput referenceAlignmentOutput, AlignmentOutput tumorAlignmentOutput) {
        return builder().maybeReference(referenceAlignmentOutput).maybeTumor(tumorAlignmentOutput).build();
    }
}
