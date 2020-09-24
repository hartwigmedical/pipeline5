package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.RunMetadata;

public interface PersistedDataset {

    Optional<String> file(final RunMetadata metadata, final DataType dataType);

    Optional<String> directory(final RunMetadata metadata, final DataType dataType);
}