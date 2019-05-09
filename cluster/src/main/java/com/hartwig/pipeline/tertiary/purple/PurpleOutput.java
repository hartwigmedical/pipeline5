package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.cobalt.ImmutableCobaltOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutput extends StageOutput {

    @Override
    default String name() {
        return Purple.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutablePurpleOutput.Builder builder() {
        return ImmutablePurpleOutput.builder();
    }
}
