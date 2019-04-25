package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SomaticCallerOutput {

    GoogleStorageLocation finalSomaticVcf();

    static ImmutableSomaticCallerOutput.Builder builder() {
        return ImmutableSomaticCallerOutput.builder();
    }
}
