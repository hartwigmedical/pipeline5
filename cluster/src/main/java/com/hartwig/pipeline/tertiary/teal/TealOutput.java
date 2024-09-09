package com.hartwig.pipeline.tertiary.teal;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface TealOutput extends StageOutput {

    @Override
    default String name() { return Teal.NAMESPACE; }

    Optional<TealOutputLocations> maybeOutputLocations();

    default TealOutputLocations outputLocations() {
        return maybeOutputLocations().orElse(TealOutputLocations.builder()
                .germlineTellength(GoogleStorageLocation.empty())
                .somaticTellength(GoogleStorageLocation.empty())
                .somaticBreakend(GoogleStorageLocation.empty())
                .build());
    }

    static ImmutableTealOutput.Builder builder() {
        return ImmutableTealOutput.builder();
    }
}
