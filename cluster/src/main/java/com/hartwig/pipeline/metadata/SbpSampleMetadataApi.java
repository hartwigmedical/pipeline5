package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import java.io.IOException;

import com.hartwig.pipeline.sbpapi.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpSample;

public class SbpSampleMetadataApi implements SampleMetadataApi {

    private static final String SAMPLE_NOT_FOUND = "sample not found";
    static final String DONE_PIPELINE_V5 = "Done_PipelineV5";
    static final String FAILED_PIPELINE_V5 = "Failed_PipelineV5";

    private final SbpRestApi sbpRestApi;
    private final int sampleEntityId;

    SbpSampleMetadataApi(final SbpRestApi sbpRestApi, final int sampleId) {
        this.sbpRestApi = sbpRestApi;
        this.sampleEntityId = sampleId;
    }

    @Override
    public SingleSampleRunMetadata get() {
        try {
            String sampleJson = sbpRestApi.getSample(sampleEntityId);
            if (sampleJson.contains(SAMPLE_NOT_FOUND)) {
                throw new IllegalArgumentException(format("No sample found for sample id [%s]", sampleEntityId));
            }
            SbpSample sample = ObjectMappers.get().readValue(sampleJson, SbpSample.class);
            return SingleSampleRunMetadata.builder()
                    .entityId(sampleEntityId)
                    .sampleId(sample.barcode())
                    .sampleName(sample.name())
                    .type(sample.type().equals("ref")
                            ? SingleSampleRunMetadata.SampleType.REFERENCE
                            : SingleSampleRunMetadata.SampleType.TUMOR)
                    .build();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
