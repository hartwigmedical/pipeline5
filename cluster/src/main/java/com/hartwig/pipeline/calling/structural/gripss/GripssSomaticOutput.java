package com.hartwig.pipeline.calling.structural.gripss;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssSomaticOutput extends GripssOutput {

    @Override
    default String name() {
        return GripssSomatic.NAMESPACE;
    }

    static ImmutableGripssSomaticOutput.Builder builder() {
        return ImmutableGripssSomaticOutput.builder();
    }
}
