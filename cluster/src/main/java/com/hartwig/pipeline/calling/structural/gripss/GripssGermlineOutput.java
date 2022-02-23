package com.hartwig.pipeline.calling.structural.gripss;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssGermlineOutput extends GripssOutput {

    @Override
    default String name() {
        return GripssGermline.NAMESPACE;
    }

    static ImmutableGripssGermlineOutput.Builder builder() {
        return ImmutableGripssGermlineOutput.builder();
    }
}
