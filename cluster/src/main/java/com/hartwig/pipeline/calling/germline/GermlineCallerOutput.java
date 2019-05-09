package com.hartwig.pipeline.calling.germline;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput extends StageOutput{

    @Override
    default String name() {
        return GermlineCaller.NAMESPACE;
    }

    static ImmutableGermlineCallerOutput.Builder builder() {
        return ImmutableGermlineCallerOutput.builder();
    }
}