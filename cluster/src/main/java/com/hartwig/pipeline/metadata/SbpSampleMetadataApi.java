package com.hartwig.pipeline.metadata;

import static java.lang.String.format;
import static java.lang.String.valueOf;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SbpSampleMetadataApi implements SampleMetadataApi {

    private static final String SAMPLE_NOT_FOUND = "sample not found";
    static final String DONE_PIPELINE_V5 = "Done_PipelineV5";
    static final String FAILED_PIPELINE_V5 = "Failed_PipelineV5";

    private final SBPRestApi sbpRestApi;
    private final int sampleId;

    SbpSampleMetadataApi(final SBPRestApi sbpRestApi, final int sampleId) {
        this.sbpRestApi = sbpRestApi;
        this.sampleId = sampleId;
    }

    @Override
    public SampleMetadata get() {
        try {
            String sampleJson = sbpRestApi.getSample(sampleId);
            if (sampleJson.contains(SAMPLE_NOT_FOUND)) {
                throw new IllegalArgumentException(format("No sample found for sample id [%s]", sampleId));
            }
            SbpSample sample = ObjectMappers.get().readValue(sampleJson, SbpSample.class);
            List<SbpSet> sbpSets = ObjectMappers.get().readValue(sbpRestApi.getSet(sampleId), new TypeReference<List<SbpSet>>() {
            });
            SbpSet first = sbpSets.stream()
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(format(
                            "No set found for sample id [%s]. This points to some inconsistency in the SBP database.",
                            sampleId)));
            return SampleMetadata.builder().barcodeOrSampleName(sample.barcode()).setName(first.name()).build();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void complete(PipelineStatus status) {
        sbpRestApi.updateStatus(SBPRestApi.SAMPLES,
                valueOf(sampleId),
                status == PipelineStatus.SUCCESS ? DONE_PIPELINE_V5 : FAILED_PIPELINE_V5);
    }
}
