package com.hartwig.pipeline.transfer.staged;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.api.SetApi;
import com.hartwig.api.helpers.OnlyOne;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.SampleSet;
import com.hartwig.events.ImmutablePipelineOutputBlob;
import com.hartwig.events.ImmutablePipelineStaged;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.MD5s;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.transfer.OutputIterator;

import org.jetbrains.annotations.NotNull;

public class StagedOutputPublisher {

    private final SetApi setApi;
    private final Bucket sourceBucket;
    private final Publisher publisher;
    private final ObjectMapper objectMapper;
    private final Run run;
    private final PipelineStaged.OutputTarget target;

    public StagedOutputPublisher(final SetApi setApi, final Bucket sourceBucket, final Publisher publisher, final ObjectMapper objectMapper,
            final Run run, final PipelineStaged.OutputTarget target) {
        this.setApi = setApi;
        this.sourceBucket = sourceBucket;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.run = run;
        this.target = target;
    }

    public void publish(final PipelineState state, final SomaticRunMetadata metadata) {
        if (state.status() != PipelineStatus.FAILED) {
            List<AddDatatype> addDatatypes =
                    state.stageOutputs().stream().map(StageOutput::datatypes).flatMap(List::stream).collect(Collectors.toList());
            SampleSet set = OnlyOne.of(setApi.list(metadata.set(), null, true), SampleSet.class);
            ImmutablePipelineStaged.Builder secondaryAnalysisEvent = eventBuilder(metadata, set, PipelineStaged.Analysis.SECONDARY);
            ImmutablePipelineStaged.Builder tertiaryAnalysisEvent = eventBuilder(metadata, set, PipelineStaged.Analysis.TERTIARY);
            OutputIterator.from(blob -> {
                Optional<DataType> dataType =
                        addDatatypes.stream().filter(d -> blob.getName().endsWith(d.path())).map(AddDatatype::dataType).findFirst();
                Blob blobWithMd5 = sourceBucket.get(blob.getName());
                if (isSecondary(blobWithMd5)) {
                    secondaryAnalysisEvent.addBlobs(builderWithPathComponents(metadata.tumor().sampleName(),
                            metadata.reference().sampleName(),
                            blobWithMd5.getName()).datatype(dataType.map(Object::toString))
                            .barcode(metadata.barcode())
                            .bucket(blobWithMd5.getBucket())
                            .filesize(blobWithMd5.getSize())
                            .hash(MD5s.asHex(blobWithMd5.getMd5()))
                            .build());
                } else {
                    tertiaryAnalysisEvent.addBlobs(builderWithPathComponents(metadata.tumor().sampleName(),
                            metadata.reference().sampleName(),
                            blobWithMd5.getName()).datatype(dataType.map(Object::toString))
                            .barcode(metadata.barcode())
                            .bucket(blobWithMd5.getBucket())
                            .filesize(blobWithMd5.getSize())
                            .hash(MD5s.asHex(blobWithMd5.getMd5()))
                            .build());
                }
            }, sourceBucket).iterate(metadata);
            publish(secondaryAnalysisEvent.build());
            publish(tertiaryAnalysisEvent.build());
        }
    }

    @NotNull
    public ImmutablePipelineStaged.Builder eventBuilder(final SomaticRunMetadata metadata, final SampleSet set,
            final PipelineStaged.Analysis secondary) {
        return ImmutablePipelineStaged.builder()
                .type(PipelineStaged.Type.DNA)
                .analysis(secondary)
                .target(target)
                .version(Versions.pipelineMajorMinorVersion())
                .sample(metadata.maybeTumor().orElse(metadata.reference()).sampleName())
                .runId(Optional.ofNullable(run.getId()))
                .setId(set.getId());
    }

    public boolean isSecondary(final Blob blobWithMd5) {
        return FileTypes.isBam(blobWithMd5.getName()) || FileTypes.isBai(blobWithMd5.getName()) || FileTypes.isCram(blobWithMd5.getName())
                || FileTypes.isCrai(blobWithMd5.getName());
    }

    public void publish(final PipelineStaged event) {
        if (!event.blobs().isEmpty()) {
            event.publish(publisher, objectMapper);
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
