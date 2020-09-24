package com.hartwig.pipeline.reruns;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

public class ApiPersistedDataset implements PersistedDataset {

    private final SbpRestApi restApi;
    private final ObjectMapper objectMapper;

    public ApiPersistedDataset(final SbpRestApi restApi, final ObjectMapper objectMapper) {
        this.restApi = restApi;
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<String> file(final RunMetadata metadata, final DataType dataType) {
        try {
            return objectMapper.<List<SbpFileMetadata>>readValue(restApi.getFileByBarcodeAndType(metadata.id(),
                    metadata.barcode(),
                    dataType.name().toLowerCase()), new TypeReference<List<SbpFileMetadata>>() {
            }).stream().findFirst().map(f -> String.format("%s/%s/%s", metadata.set(), f.directory(), f.filename()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<String> directory(final RunMetadata metadata, final DataType dataType) {
        return file(metadata, dataType).map(File::new).map(File::getParent);
    }
}
