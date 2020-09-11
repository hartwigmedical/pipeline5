package com.hartwig.pipeline.reruns;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiPersistedDataset implements PersistedDataset<SingleSampleRunMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiPersistedDataset.class);
    private final SbpRestApi restApi;
    private final ObjectMapper objectMapper;

    public ApiPersistedDataset(final SbpRestApi restApi, final ObjectMapper objectMapper) {
        this.restApi = restApi;
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<String> find(final SingleSampleRunMetadata metadata, final DataType dataType) {
        try {
            Optional<String> path = objectMapper.<List<SbpFileMetadata>>readValue(restApi.getFileByBarcodeAndType(metadata.id(),
                    metadata.barcode(),
                    dataType.name().toLowerCase()), new TypeReference<List<SbpFileMetadata>>() {
            }).stream().findFirst().map(f -> f.directory() + "/" + f.filename());
            LOGGER.info("Result of persisted data lookup for sample [{}] and type [{}] is [{}]", metadata.sampleName(), dataType, path);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
