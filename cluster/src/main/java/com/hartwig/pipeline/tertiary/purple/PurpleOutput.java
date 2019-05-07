package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.cobalt.ImmutableCobaltOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutput {

    JobStatus status();

    static ImmutablePurpleOutput.Builder builder() {
        return ImmutablePurpleOutput.builder();
    }
}
