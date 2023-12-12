package com.hartwig.pipeline.alignment;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentOutput extends StageOutput {

    String sample();

    @Value.Default
    default String name() {
        return Aligner.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeAlignments();

    default GoogleStorageLocation alignments() {
        return maybeAlignments().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableAlignmentOutput.Builder builder() {
        return ImmutableAlignmentOutput.builder();
    }
}
