package com.hartwig.pipeline.calling.somatic;

import java.util.Optional;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SomaticCallerOutput {

    JobStatus status();

    Optional<GoogleStorageLocation> finalSomaticVcf();

    static ImmutableSomaticCallerOutput.Builder builder() {
        return ImmutableSomaticCallerOutput.builder();
    }
}
