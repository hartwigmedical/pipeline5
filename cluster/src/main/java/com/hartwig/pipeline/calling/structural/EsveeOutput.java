package com.hartwig.pipeline.calling.structural;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface EsveeOutput extends StageOutput {

    @Override
    default String name() {
        return Esvee.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeUnfilteredVcf();

    Optional<GoogleStorageLocation> maybeUnfilteredVcfIndex();

    Optional<GoogleStorageLocation> maybeSomaticVcf();

    Optional<GoogleStorageLocation> maybeSomaticVcfIndex();

    Optional<GoogleStorageLocation> maybeGermlineVcf();

    Optional<GoogleStorageLocation> maybeGermlineVcfIndex();

    default GoogleStorageLocation unfilteredVcf() {
        return maybeUnfilteredVcf().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation somaticVcf() {
        return maybeSomaticVcf().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation germlineVcf() {
        return maybeGermlineVcf().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableEsveeOutput.Builder builder() {
        return ImmutableEsveeOutput.builder();
    }
}
