package com.hartwig.pipeline.tertiary.virus;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusBreakendOutput extends StageOutput {

    @Override
    default String name() {
        return VirusBreakend.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeSummary();

    default GoogleStorageLocation summary() {
        return maybeSummary().orElse(GoogleStorageLocation.empty());
    }

    Optional<GoogleStorageLocation> maybeVariants();

    default GoogleStorageLocation variants() {
        return maybeVariants().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableVirusBreakendOutput.Builder builder() {
        return ImmutableVirusBreakendOutput.builder();
    }
}