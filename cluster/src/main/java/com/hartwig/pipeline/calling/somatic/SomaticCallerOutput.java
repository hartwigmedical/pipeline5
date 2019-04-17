package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SomaticCallerOutput {

    GoogleStorageLocation allSomaticIndelsVcf();

    GoogleStorageLocation allSomaticSnvsVcf();

    GoogleStorageLocation passedSomaticIndelsVcf();

    GoogleStorageLocation passedSomaticSnvsVcf();

    static ImmutableSomaticCallerOutput.Builder builder() {
        return ImmutableSomaticCallerOutput.builder();
    }
}
