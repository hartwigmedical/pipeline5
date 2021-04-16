package com.hartwig.pipeline.tertiary.virusbreakend;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusBreakendOutput extends StageOutput {

    @Override
    default String name() {
        return VirusBreakend.NAMESPACE;
    }

    static ImmutableVirusBreakendOutput.Builder builder() {
        return ImmutableVirusBreakendOutput.builder();
    }
}
