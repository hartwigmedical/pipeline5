package com.hartwig.pipeline.tertiary.linx;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxGermlineOutput extends StageOutput {

    @Override
    default String name() {
        return LinxGermline.NAMESPACE;
    }

    static ImmutableLinxGermlineOutput.Builder builder() {
        return ImmutableLinxGermlineOutput.builder();
    }

    Optional<LinxGermlineOutputLocations> maybeLinxGermlineOutputLocations();
}
