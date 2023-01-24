package com.hartwig.pipeline.output;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.jackson.ObjectMappers;

public class OutputDataset {

    public static final String DATASET_JSON = "dataset.json";
    private final Map<String, Map<String, Map<String, String>>> datasetMap;
    private final Bucket outputBucket;
    private final String set;

    public OutputDataset(final Bucket outputBucket, final String set) {
        this.outputBucket = outputBucket;
        this.set = set;
        this.datasetMap = new HashMap<>();
    }

    public void add(final AddDatatype datatype, final Blob blob) {
        final Map<String, Map<String, String>> sampleMap =
                datasetMap.computeIfAbsent(datatype.dataType().toString().toLowerCase(), k -> new HashMap<>());
        sampleMap.put(datatype.barcode(),
                Map.of("is_directory",
                        Boolean.toString(datatype.isDirectory()),
                        "path",
                        blob.getName().replaceFirst(set, "").substring(1)));
    }

    public void serializeAndUpload() {
        try {
            outputBucket.create(set + "/" + DATASET_JSON, ObjectMappers.get().writeValueAsBytes(datasetMap));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}