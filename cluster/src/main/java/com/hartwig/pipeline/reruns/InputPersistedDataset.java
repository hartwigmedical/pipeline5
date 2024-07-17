package com.hartwig.pipeline.reruns;

import static java.util.Optional.ofNullable;

import java.util.Map;
import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.datatypes.DataType;

public class InputPersistedDataset implements PersistedDataset {

    private final Map<String, Map<String, Map<String, String>>> datasetMap;
    private final String billingProject;

    public InputPersistedDataset(final PipelineInput input, final String billingProject) {
        this.billingProject = billingProject;
        this.datasetMap = input.dataset();
    }

    public Optional<GoogleStorageLocation> path(final String sample, final DataType dataType) {
        return ofNullable(datasetMap.get(dataType.toString().toLowerCase())).map(d -> d.get(sample))
                .map(d -> d.get("path"))
                .map(p -> GoogleStorageLocation.from(p, billingProject));
    }
}
