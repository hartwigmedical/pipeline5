package com.hartwig.pipeline.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;

public class TestPersistedDataset implements PersistedDataset {

    private final Map<DataType, String> files;
    private final Map<DataType, String> directories;

    public TestPersistedDataset() {
        this.files = new HashMap<>();
        this.directories = new HashMap<>();
    }

    public void addPath(final DataType dataType, final String path) {
        files.put(dataType, path);
    }

    public void addDir(final DataType dataType, final String path) {
        directories.put(dataType, path);
    }

    @Override
    public Optional<String> file(final RunMetadata metadata, final DataType dataType) {
        return Optional.ofNullable(files.get(dataType));
    }

    @Override
    public Optional<String> directory(final RunMetadata metadata, final DataType dataType) {
        return Optional.ofNullable(directories.get(dataType));
    }
}
