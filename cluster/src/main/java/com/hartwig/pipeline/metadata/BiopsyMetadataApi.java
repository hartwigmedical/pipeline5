package com.hartwig.pipeline.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Bucket;
import com.hartwig.api.SampleApi;
import com.hartwig.api.SetApi;
import com.hartwig.api.helpers.OnlyOne;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleSet;
import com.hartwig.api.model.SampleType;
import com.hartwig.events.ImmutablePipelineOutputBlob;
import com.hartwig.events.ImmutablePipelineStaged;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.transfer.OutputIterator;

import org.jetbrains.annotations.NotNull;

public class BiopsyMetadataApi implements SomaticMetadataApi {

    private final SampleApi sampleApi;
    private final SetApi setApi;
    private final String biopsyName;
    private final Arguments arguments;
    private final Publisher publisher;
    private final ObjectMapper objectMapper;
    private final Bucket sourceBucket;

    public BiopsyMetadataApi(final SampleApi sampleApi, final SetApi setApi, final String biopsyName, final Arguments arguments,
            final Publisher publisher, final ObjectMapper objectMapper, final Bucket sourceBucket) {
        this.sampleApi = sampleApi;
        this.setApi = setApi;
        this.biopsyName = biopsyName;
        this.arguments = arguments;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.sourceBucket = sourceBucket;
    }

    @Override
    public SomaticRunMetadata get() {
        Sample tumor = OnlyOne.of(sampleApi.list(null, null, null, null, SampleType.TUMOR, biopsyName), Sample.class);
        SampleSet set = OnlyOne.of(setApi.list(null, tumor.getId(), true), SampleSet.class);
        Sample ref = OnlyOne.of(sampleApi.list(null, null, null, set.getId(), SampleType.REF, null), Sample.class);
        return SomaticRunMetadata.builder()
                .bucket(arguments.outputBucket())
                .set(set.getName())
                .maybeTumor(singleSample(tumor, SingleSampleRunMetadata.SampleType.TUMOR))
                .reference(singleSample(ref, SingleSampleRunMetadata.SampleType.REFERENCE))
                .build();
    }

    @NotNull
    public ImmutableSingleSampleRunMetadata singleSample(final Sample sample, final SingleSampleRunMetadata.SampleType type) {
        return SingleSampleRunMetadata.builder()
                .type(type)
                .barcode(sample.getBarcode())
                .set(biopsyName)
                .sampleName(sample.getName())
                .bucket(arguments.outputBucket())
                .build();
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void complete(final PipelineState state, final SomaticRunMetadata metadata) {
        if (state.status() != PipelineStatus.FAILED) {
            List<AddDatatype> addDatatypes =
                    state.stageOutputs().stream().map(StageOutput::datatypes).flatMap(List::stream).collect(Collectors.toList());
            SampleSet set = OnlyOne.of(setApi.list(metadata.set(), null, null), SampleSet.class);
            ImmutablePipelineStaged.Builder stagedEventBuilder = ImmutablePipelineStaged.builder()
                    .type(PipelineStaged.Type.DNA)
                    .version(Versions.pipelineMajorMinorVersion())
                    .sample(metadata.tumor().sampleName())
                    .setId(set.getId());
            OutputIterator.from(blob -> {
                Optional<DataType> dataType =
                        addDatatypes.stream().filter(d -> blob.getName().endsWith(d.path())).map(AddDatatype::dataType).findFirst();
                stagedEventBuilder.addBlobs(builderWithPathComponents(metadata.tumor().sampleName(),
                        metadata.reference().sampleName(),
                        blob.getName()).datatype(dataType.map(Object::toString))
                        .barcode(metadata.barcode())
                        .bucket(blob.getBucket())
                        .filesize(blob.getSize())
                        .hash(MD5s.convertMd5ToSbpFormat(blob.getMd5()))
                        .build());
            }, sourceBucket).iterate(metadata);
            stagedEventBuilder.build().publish(publisher, objectMapper);
        }
    }

    private static ImmutablePipelineOutputBlob.Builder builderWithPathComponents(final String tumorSample, final String refSample,
            final String blobName) {
        ImmutablePipelineOutputBlob.Builder outputBlob = PipelineOutputBlob.builder();
        String[] splitName = blobName.split("/");
        boolean rootFile = splitName.length == 2;
        boolean singleSample = splitName.length > 3 && (splitName[1].equals(tumorSample) || splitName[1].equals(refSample));
        if (rootFile) {
            outputBlob.root(splitName[0]).filename(splitName[1]);
        } else if (singleSample) {
            outputBlob.root(splitName[0])
                    .sampleSubdir(splitName[1])
                    .namespace(splitName[2])
                    .filename(String.join("/", Arrays.copyOfRange(splitName, 3, splitName.length)));
        } else {
            outputBlob.root(splitName[0])
                    .namespace(splitName[1])
                    .filename(String.join("/", Arrays.copyOfRange(splitName, 2, splitName.length)));
        }
        return outputBlob;
    }
}
