package com.hartwig.pipeline.metadata;

import static java.lang.String.format;
import static java.lang.String.valueOf;

import java.io.IOException;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SbpSampleMetadataApi implements SampleMetadataApi {

    private static final String SAMPLE_NOT_FOUND = "sample not found";
    static final String ALIGNMENT_DONE_PIPELINE_V5 = "AlignmentDone_PipelineV5";
    static final String DONE_PIPELINE_V5 = "Done_PipelineV5";
    static final String FAILED_PIPELINE_V5 = "Failed_PipelineV5";

    private final SBPRestApi sbpRestApi;
    private final int sampleId;

    SbpSampleMetadataApi(final SBPRestApi sbpRestApi, final int sampleId) {
        this.sbpRestApi = sbpRestApi;
        this.sampleId = sampleId;
    }

    @Override
    public SingleSampleRunMetadata get() {
        try {
            String sampleJson = sbpRestApi.getSample(sampleId);
            if (sampleJson.contains(SAMPLE_NOT_FOUND)) {
                throw new IllegalArgumentException(format("No sample found for sample id [%s]", sampleId));
            }
            SbpSample sample = ObjectMappers.get().readValue(sampleJson, SbpSample.class);
            return SingleSampleRunMetadata.builder()
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

    @Override
    public void alignmentComplete(PipelineStatus status){
        sbpRestApi.updateSampleStatus(valueOf(sampleId), status == PipelineStatus.SUCCESS ? ALIGNMENT_DONE_PIPELINE_V5 : FAILED_PIPELINE_V5);
    }

    @Override
    public void complete(PipelineStatus status) {
        sbpRestApi.updateSampleStatus(valueOf(sampleId), status == PipelineStatus.SUCCESS ? DONE_PIPELINE_V5 : FAILED_PIPELINE_V5);
    }
}
