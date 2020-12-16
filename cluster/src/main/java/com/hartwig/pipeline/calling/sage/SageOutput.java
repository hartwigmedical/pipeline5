package com.hartwig.pipeline.calling.sage;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SageOutput extends StageOutput {

    PipelineStatus status();

    Optional<GoogleStorageLocation> maybeFinalVcf();

    default GoogleStorageLocation finalVcf() {
        return maybeFinalVcf().orElseThrow(() -> new IllegalStateException("No final vcf available"));
    }

    static ImmutableSageOutput.Builder builder(String nameSpace) {
        return ImmutableSageOutput.builder().name(nameSpace);
    }
}
