package com.hartwig.pipeline.metadata;

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
}