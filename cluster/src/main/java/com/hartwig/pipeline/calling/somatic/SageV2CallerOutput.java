package com.hartwig.pipeline.calling.somatic;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SageV2CallerOutput extends StageOutput {

    default String name() {
        return SageV2Caller.NAMESPACE;
    }

    PipelineStatus status();

    Optional<GoogleStorageLocation> maybeFinalSomaticVcf();

    default GoogleStorageLocation finalSomaticVcf() {
        return maybeFinalSomaticVcf().orElseThrow(() -> new IllegalStateException("No final somatic vcf available"));
    }

    static ImmutableSageV2CallerOutput.Builder builder() {
        return ImmutableSageV2CallerOutput.builder();
    }
}
