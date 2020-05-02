package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.execution.PipelineStatus;

public class LocalSomaticMetadataApi implements SomaticMetadataApi {

    private final Arguments arguments;

    LocalSomaticMetadataApi(final Arguments arguments) {
        this.arguments = arguments;
    }

    @Override
    public SomaticRunMetadata get() {

        String setId = RunTag.apply(arguments, arguments.setId());
        return SomaticRunMetadata.builder()
                .runName(setId)
                .maybeTumor(SingleSampleRunMetadata.builder()
                        .type(SingleSampleRunMetadata.SampleType.TUMOR)
                        .sampleId(arguments.setId() + "T")
                        .sampleName(arguments.setId() + "T")
                        .build())
                .reference(SingleSampleRunMetadata.builder()
                        .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                        .sampleId(arguments.setId() + "R")
                        .sampleName(arguments.setId() + "R")
                        .build())
                .build();
    }

    @Override
    public void complete(final PipelineStatus status, SomaticRunMetadata metadata, PipelineState state) {
        // do nothing
    }
}