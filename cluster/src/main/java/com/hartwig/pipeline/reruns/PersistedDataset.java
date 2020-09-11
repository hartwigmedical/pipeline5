package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.RunMetadata;

public interface PersistedDataset<T extends RunMetadata> {

    Optional<String> find(final T metadata, final DataType dataType);
}