package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.immutables.value.Value;

@Value.Immutable
public interface SageV2CallerOutput extends StageOutput {

    default String name() {
        return SageV2Caller.NAMESPACE;
    }

    PipelineStatus status();

    static ImmutableSageV2CallerOutput.Builder builder() {
        return ImmutableSageV2CallerOutput.builder();
    }
}
