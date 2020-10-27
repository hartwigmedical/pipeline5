package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class NoopPersistedDataset implements PersistedDataset {
    @Override
    public Optional<GoogleStorageLocation> path(final String sampleName, final DataType dataType) {
        return Optional.empty();
    }
}