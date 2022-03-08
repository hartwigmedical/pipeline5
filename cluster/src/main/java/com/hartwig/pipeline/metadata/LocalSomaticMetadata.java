package com.hartwig.pipeline.metadata;

import java.util.Optional;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata.SampleType;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

public class LocalSomaticMetadata implements SomaticMetadataApi {

    private final Arguments arguments;
    private final JsonSampleSource jsonSampleSource;
    private final StagedOutputPublisher stagedOutputPublisher;

    LocalSomaticMetadata(final Arguments arguments, final JsonSampleSource jsonSampleSource, final StagedOutputPublisher stagedOutputPublisher) {
        this.arguments = arguments;
        this.jsonSampleSource = jsonSampleSource;
        this.stagedOutputPublisher = stagedOutputPublisher;
    }

    @Override
    public SomaticRunMetadata get() {
        String setId = RunTag.apply(arguments, arguments.setId());
        Optional<Sample> reference = jsonSampleSource.sample(SampleType.REFERENCE);
        Optional<Sample> tumor = jsonSampleSource.sample(SampleType.TUMOR);

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

    @Override
    public void complete(final PipelineState state, final SomaticRunMetadata metadata) {
        if (arguments.publishDbLoadEvent()) {
            stagedOutputPublisher.publish(state, metadata);
        }
    }

    @Override
    public void start() {
        // do nothing
    }
}