package com.hartwig.pipeline.metadata;

public class LocalSampleMetadataApi extends SingleSampleEventListener implements SampleMetadataApi {

    private final String sampleId;

    public LocalSampleMetadataApi(final String sampleId) {
        this.sampleId = sampleId;
    }

    public SingleSampleRunMetadata get() {
        return SingleSampleRunMetadata.builder()
                .entityId(-1)
                .sampleId(sampleId)
                .type(sampleId.toUpperCase().endsWith("T")
                        ? SingleSampleRunMetadata.SampleType.TUMOR
                        : SingleSampleRunMetadata.SampleType.REFERENCE)
                .build();
    }
}