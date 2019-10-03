package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutput extends StageOutput {

    @Override
    default String name() {
        return Purple.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    Optional<GoogleStorageLocation> maybeSomaticVcf();

    Optional<GoogleStorageLocation> maybeStructuralVcf();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    default GoogleStorageLocation somaticVcf() {
        return maybeSomaticVcf().orElseThrow(() -> new IllegalStateException("No somatic vcf available"));
    }

    default GoogleStorageLocation structuralVcf() {
        return maybeStructuralVcf().orElseThrow(() -> new IllegalStateException("No structural vcf available"));
    }

    static ImmutablePurpleOutput.Builder builder() {
        return ImmutablePurpleOutput.builder();
    }
}
