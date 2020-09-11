package com.hartwig.pipeline.metadata;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata.SampleType;

public class LocalSomaticMetadata implements SomaticMetadataApi {

    private final Arguments arguments;
    private JsonSampleSource jsonSampleSource;

    LocalSomaticMetadata(final Arguments arguments, JsonSampleSource jsonSampleSource) {
        this.arguments = arguments;
        this.jsonSampleSource = jsonSampleSource;
    }

    @Override
    public SomaticRunMetadata get() {
        String setId = RunTag.apply(arguments, arguments.setId());
        Sample reference = jsonSampleSource.sample(SampleType.REFERENCE);
        Sample tumor = jsonSampleSource.sample(SampleType.TUMOR);

        return SomaticRunMetadata.builder()
                .set(setId)
                .id(setId)
                .bucket(arguments.outputBucket())
                .maybeTumor(SingleSampleRunMetadata.builder()
                        .id(setId)
                        .bucket(arguments.outputBucket())
                        .set(setId)
                        .type(SingleSampleRunMetadata.SampleType.TUMOR)
                        .barcode(tumor.name())
                        .sampleName(tumor.name())
                        .build())
                .reference(SingleSampleRunMetadata.builder()
                        .id(setId)
                        .bucket(arguments.outputBucket())
                        .set(setId)
                        .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                        .barcode(reference.name())
                        .sampleName(reference.name())
                        .build())
                .build();
    }

    @Override
    public void complete(final PipelineState state, SomaticRunMetadata metadata) {
        // do nothing
    }
}