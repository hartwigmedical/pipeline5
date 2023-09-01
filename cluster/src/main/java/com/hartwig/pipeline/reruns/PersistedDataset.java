package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.datatypes.DataType;

public interface PersistedDataset {
    Optional<GoogleStorageLocation> path(final String sample, final DataType dataType);
}
