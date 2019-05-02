package com.hartwig.pipeline.tertiary;

import java.util.Optional;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AmberOutput {

    JobStatus status();

    Optional<GoogleStorageLocation> baf();

    static ImmutableAmberOutput.Builder builder() {
        return ImmutableAmberOutput.builder();
    }
}
