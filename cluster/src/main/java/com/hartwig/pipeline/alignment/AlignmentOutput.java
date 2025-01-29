package com.hartwig.pipeline.alignment;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentOutput extends StageOutput {

    String sample();

    @Value.Default
    default String name() {
        return Aligner.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeAlignments();

    Optional<GoogleStorageLocation> maybeJitterParams();

    Optional<GoogleStorageLocation> maybeMsTable();

    default GoogleStorageLocation alignments() {
        return maybeAlignments().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation jitterParams() {
        return maybeJitterParams().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation msTable() { return maybeMsTable().orElse(GoogleStorageLocation.empty()); }

    static ImmutableAlignmentOutput.Builder builder() {
        return ImmutableAlignmentOutput.builder();
    }
}
