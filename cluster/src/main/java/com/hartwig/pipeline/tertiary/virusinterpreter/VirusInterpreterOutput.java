package com.hartwig.pipeline.tertiary.virusinterpreter;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.tertiary.virusbreakend.VirusBreakend;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusInterpreterOutput extends StageOutput {

    @Override
    default String name() {
        return VirusBreakend.NAMESPACE;
    }

    static ImmutableVirusInterpreterOutput.Builder builder() {
        return ImmutableVirusInterpreterOutput.builder();
    }
}
