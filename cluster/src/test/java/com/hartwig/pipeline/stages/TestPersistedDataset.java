package com.hartwig.pipeline.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class TestPersistedDataset implements PersistedDataset {

    private final Map<DataType, String> files;

    public TestPersistedDataset() {
        this.files = new HashMap<>();
    }

    public void addPath(final DataType dataType, final String path) {
        files.put(dataType, path);
    }

    @Override
    public Optional<GoogleStorageLocation> path(final String sampleName, final DataType dataType) {
        return Optional.ofNullable(files.get(dataType)).map(f -> GoogleStorageLocation.of("bucket", f));
    }
}
