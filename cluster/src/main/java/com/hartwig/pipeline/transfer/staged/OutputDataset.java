package com.hartwig.pipeline.transfer.staged;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.api.model.DatasetFile;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.AddDatatype;

public class OutputDataset {

    public static final String DATASET_JSON = "dataset.json";
    private final Map<String, Map<String, DatasetFile>> datasetMap;
    private final Bucket outputBucket;
    private final String set;

    public OutputDataset(final Bucket outputBucket, final String set) {
        this.outputBucket = outputBucket;
        this.set = set;
        this.datasetMap = new HashMap<>();
    }

    public void add(final AddDatatype datatype, final Blob blob) {
        final Map<String, DatasetFile> sampleMap = datasetMap.computeIfAbsent(datatype.dataType().toString(), k -> new HashMap<>());
        sampleMap.put(datatype.barcode(), new DatasetFile().path(blob.getName()));
    }

    public void serialize() {
        try {
            outputBucket.create(set + "/" + DATASET_JSON, ObjectMappers.get().writeValueAsBytes(datasetMap));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}