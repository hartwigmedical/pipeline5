package com.hartwig.pipeline.calling.structural.esvee;

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

    Optional<GoogleStorageLocation> maybeUnfilteredVcfLocation();

    Optional<GoogleStorageLocation> maybeUnfilteredVcfIndexLocation();

    Optional<GoogleStorageLocation> maybeSomaticVcfLocation();

    Optional<GoogleStorageLocation> maybeSomaticVcfIndexLocation();

    Optional<GoogleStorageLocation> maybeGermlineVcfLocation();

    Optional<GoogleStorageLocation> maybeGermlineVcfIndexLocation();

    default GoogleStorageLocation unfilteredVcfFile() {
        return maybeUnfilteredVcfLocation().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation somaticVcfFile() {
        return maybeSomaticVcfLocation().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation germlineVcfFile() {
        return maybeGermlineVcfLocation().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableEsveeOutput.Builder builder() {
        return ImmutableEsveeOutput.builder();
    }
}
