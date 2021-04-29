package com.hartwig.pipeline.metadata;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.api.SampleApi;
import com.hartwig.api.SetApi;
import com.hartwig.api.helpers.OnlyOne;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleSet;
import com.hartwig.api.model.SampleType;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.jetbrains.annotations.NotNull;

public class ResearchMetadataApi implements SomaticMetadataApi {

    private final SampleApi sampleApi;
    private final SetApi setApi;
    private final String biopsyName;
    private final Arguments arguments;
    private final StagedOutputPublisher stagedOutput;
    private final Anonymizer anonymizer;

    public ResearchMetadataApi(final SampleApi sampleApi, final SetApi setApi, final String biopsyName, final Arguments arguments,
            final StagedOutputPublisher stagedOutput, final Anonymizer anonymizer) {
        this.sampleApi = sampleApi;
        this.setApi = setApi;
        this.biopsyName = biopsyName;
        this.arguments = arguments;
        this.stagedOutput = stagedOutput;
        this.anonymizer = anonymizer;
    }

    @Override
    public SomaticRunMetadata get() {
        List<Sample> possibleTumors = sampleApi.list(null, null, null, null, SampleType.TUMOR, biopsyName);
        SampleSet set = possibleTumors.stream()
                .flatMap(sample -> setApi.list(null, sample.getId(), true).stream())
                .collect(Collectors.toList())
                .stream()
                .max(Comparator.comparing(SampleSet::getName))
                .orElseThrow(() -> new IllegalStateException(String.format("No viable set found for biopsy [%s]", biopsyName)));

        Sample ref = OnlyOne.of(sampleApi.list(null, null, null, set.getId(), SampleType.REF, null), Sample.class);
        Sample tumor = OnlyOne.of(sampleApi.list(null, null, null, set.getId(), SampleType.TUMOR, null), Sample.class);
        return SomaticRunMetadata.builder()
                .bucket(arguments.outputBucket())
                .set(set.getName())
                .maybeTumor(singleSample(tumor, SingleSampleRunMetadata.SampleType.TUMOR, set.getName()))
                .reference(singleSample(ref, SingleSampleRunMetadata.SampleType.REFERENCE, set.getName()))
                .build();
    }

    @NotNull
    public ImmutableSingleSampleRunMetadata singleSample(final Sample sample, final SingleSampleRunMetadata.SampleType type,
            final String set) {
        return SingleSampleRunMetadata.builder()
                .type(type)
                .barcode(sample.getBarcode())
                .set(set)
                .sampleName(anonymizer.sampleName(sample))
                .bucket(arguments.outputBucket())
                .build();
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void complete(final PipelineState state, final SomaticRunMetadata metadata) {
        stagedOutput.publish(state, metadata);
    }
}
