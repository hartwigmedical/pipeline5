package com.hartwig.pipeline.alignment.merge;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface IndexedBamLocation {

    GoogleStorageLocation bam();

    GoogleStorageLocation bai();

    static ImmutableIndexedBamLocation.Builder builder () {
        return ImmutableIndexedBamLocation.builder();
    }
}
