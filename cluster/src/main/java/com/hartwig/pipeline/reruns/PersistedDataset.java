package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

public interface PersistedDataset {
    Optional<GoogleStorageLocation> path(final String sample, final DataType dataType);
}
