package com.hartwig.pipeline.tertiary.virus;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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