package com.hartwig.pipeline.input;

import java.util.Optional;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;

public class MetadataProvider {

    private final Arguments arguments;
    private final PipelineInput pipelineInput;

    public MetadataProvider(final Arguments arguments, final PipelineInput pipelineInput) {
        this.arguments = arguments;
        this.pipelineInput = pipelineInput;
    }

    public SomaticRunMetadata get() {
        String setId = RunTag.apply(arguments, arguments.setId());
        Optional<Sample> reference = pipelineInput.sample(SampleType.REFERENCE);
        Optional<Sample> tumor = pipelineInput.sample(SampleType.TUMOR);

        return SomaticRunMetadata.builder()
                .set(setId)
                .bucket(arguments.outputBucket())
                .maybeTumor(tumor.map(t -> SingleSampleRunMetadata.builder()
                        .bucket(arguments.outputBucket())
                        .set(setId)
                        .type(SingleSampleRunMetadata.SampleType.TUMOR)
                        .barcode(t.name())
                        .sampleName(t.name())
                        .primaryTumorDoids(t.primaryTumorDoids())
                        .build()))
                .maybeReference(reference.map(r -> SingleSampleRunMetadata.builder()
                        .bucket(arguments.outputBucket())
                        .set(setId)
                        .type(SingleSampleRunMetadata.SampleType.REFERENCE)
                        .barcode(r.name())
                        .sampleName(r.name())
                        .build()))
                .build();
    }

}