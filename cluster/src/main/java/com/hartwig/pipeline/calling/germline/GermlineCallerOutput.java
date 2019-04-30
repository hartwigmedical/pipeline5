package com.hartwig.pipeline.calling.germline;

import java.util.Optional;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput {

    JobStatus status();

    Optional<GoogleStorageLocation> germlineVcf();

    static ImmutableGermlineCallerOutput.Builder builder() {
        return ImmutableGermlineCallerOutput.builder();
    }
}
