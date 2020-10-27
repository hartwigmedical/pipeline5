package com.hartwig.pipeline.reruns;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.RunMetadata;

public class NoopPersistedDataset implements PersistedDataset {
    @Override
    public Optional<String> file(final RunMetadata metadata, final DataType dataType) {
        return Optional.empty();
    }

    @Override
    public Optional<String> directory(final RunMetadata metadata, final DataType dataType) {
        return Optional.empty();
    }
}
