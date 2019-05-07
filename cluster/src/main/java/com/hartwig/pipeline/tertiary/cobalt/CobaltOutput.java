package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CobaltOutput {

    JobStatus status();

    GoogleStorageLocation outputDirectory();

    static ImmutableCobaltOutput.Builder builder() {
        return ImmutableCobaltOutput.builder();
    }
}
