package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public class LocalSampleMetadataApi implements SampleMetadataApi {

    private final String sampleId;

    LocalSampleMetadataApi(final String sampleId) {
        this.sampleId = sampleId;
    }

    public SingleSampleRunMetadata get() {
        return SingleSampleRunMetadata.builder()
                .sampleId(sampleId)
                .type(sampleId.toUpperCase().endsWith("T")
                        ? SingleSampleRunMetadata.SampleType.TUMOR
                        : SingleSampleRunMetadata.SampleType.REFERENCE)
                .build();
    }

    @Override
    public void complete(PipelineStatus status) {
        // do nothing
    }
}