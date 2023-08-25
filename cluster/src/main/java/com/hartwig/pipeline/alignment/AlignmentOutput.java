package com.hartwig.pipeline.alignment;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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
