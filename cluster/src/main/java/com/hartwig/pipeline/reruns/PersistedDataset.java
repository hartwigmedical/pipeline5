package com.hartwig.pipeline.reruns;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.datatypes.DataType;

import java.util.Optional;

public interface PersistedDataset {
    Optional<GoogleStorageLocation> path(final String sample, final DataType dataType);
}
