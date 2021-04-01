package com.hartwig.pipeline.tertiary.virusbreakend;

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

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutableVirusBreakendOutput.Builder builder() {
        return ImmutableVirusBreakendOutput.builder();
    }
}
