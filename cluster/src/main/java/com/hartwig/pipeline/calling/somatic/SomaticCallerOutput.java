package com.hartwig.pipeline.calling.somatic;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SomaticCallerOutput extends StageOutput {

    default String name() {
        return SomaticCaller.NAMESPACE;
    }

    JobStatus status();

    Optional<GoogleStorageLocation> maybeFinalSomaticVcf();

    default GoogleStorageLocation finalSomaticVcf() {
        return maybeFinalSomaticVcf().orElseThrow(() -> new IllegalStateException("No final somatic vcf available"));
    }

    static ImmutableSomaticCallerOutput.Builder builder() {
        return ImmutableSomaticCallerOutput.builder();
    }
}
