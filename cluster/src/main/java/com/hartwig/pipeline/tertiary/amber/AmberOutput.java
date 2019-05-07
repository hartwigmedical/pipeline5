package com.hartwig.pipeline.tertiary.amber;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AmberOutput {

    JobStatus status();

    GoogleStorageLocation outputDirectory();

    static ImmutableAmberOutput.Builder builder() {
        return ImmutableAmberOutput.builder();
    }
}
