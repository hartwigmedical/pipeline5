package com.hartwig.pipeline.transfer.staged;

import static java.util.function.Predicate.not;

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
import com.hartwig.events.Analysis;
import com.hartwig.events.Analysis.Molecule;
import com.hartwig.events.Analysis.Type;
import com.hartwig.events.ImmutablePipelineOutputBlob;
import com.hartwig.events.ImmutablePipelineStaged;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.MD5s;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.transfer.OutputIterator;

import org.jetbrains.annotations.NotNull;

public class StagedOutputPublisher {

    private final SetApi setApi;
    private final Bucket sourceBucket;
    private final Publisher publisher;
    private final ObjectMapper objectMapper;
    private final Run run;
    private final Analysis.Context context;
    private final boolean stageCrams;
    private final boolean useOnlyDBSets;

    public StagedOutputPublisher(final SetApi setApi, final Bucket sourceBucket, final Publisher publisher, final ObjectMapper objectMapper,
            final Run run, final Analysis.Context target, final boolean stageCrams, final boolean useOnlyDBSets) {
        this.setApi = setApi;
        this.sourceBucket = sourceBucket;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.run = run;
        this.context = target;
        this.stageCrams = stageCrams;
        this.useOnlyDBSets = useOnlyDBSets;
    }

    public void publish(final PipelineState state, final SomaticRunMetadata metadata) {
        if (state.status() != PipelineStatus.FAILED) {
            List<AddDatatype> addDatatypes =
                    state.stageOutputs().stream().map(StageOutput::datatypes).flatMap(List::stream).collect(Collectors.toList());
            SampleSet set = OnlyOne.of(setApi.list(metadata.set(), null, useOnlyDBSets ? true : null), SampleSet.class);
            String sampleName = metadata.maybeTumor().orElse(metadata.reference()).sampleName();
            ImmutablePipelineStaged.Builder secondaryAnalysisEvent = eventBuilder(set, Type.SECONDARY, sampleName);
            ImmutablePipelineStaged.Builder tertiaryAnalysisEvent = eventBuilder(set, Type.TERTIARY, sampleName);
            ImmutablePipelineStaged.Builder germlineAnalysisEvent = eventBuilder(set, Type.GERMLINE, sampleName);

            OutputIterator.from(blob -> {
                Optional<DataType> dataType =
                        addDatatypes.stream().filter(d -> blob.getName().endsWith(d.path())).map(AddDatatype::dataType).findFirst();
                Blob blobWithMd5 = sourceBucket.get(blob.getName());
                if (isSecondary(blobWithMd5)) {
                    secondaryAnalysisEvent.addBlobs(builderWithPathComponents(sampleName,
                            metadata.reference().sampleName(),
                            blobWithMd5.getName()).datatype(dataType.map(Object::toString))
                            .barcode(metadata.barcode())
                            .bucket(blobWithMd5.getBucket())
                            .filesize(blobWithMd5.getSize())
                            .hash(MD5s.asHex(blobWithMd5.getMd5()))
                            .build());
                } else {
                    if (isGermline(blobWithMd5)) {
                        germlineAnalysisEvent.addBlobs(builderWithPathComponents(metadata.tumor().sampleName(),
                                metadata.reference().sampleName(),
                                blobWithMd5.getName()).datatype(dataType.map(Object::toString))
                                .barcode(metadata.barcode())
                                .bucket(blobWithMd5.getBucket())
                                .filesize(blobWithMd5.getSize())
                                .hash(MD5s.asHex(blobWithMd5.getMd5()))
                                .build());
                    } else if (notSecondary(blobWithMd5)) {
                        tertiaryAnalysisEvent.addBlobs(builderWithPathComponents(metadata.tumor().sampleName(),
                                metadata.reference().sampleName(),
                                blobWithMd5.getName()).datatype(dataType.map(Object::toString))
                                .barcode(metadata.barcode())
                                .bucket(blobWithMd5.getBucket())
                                .filesize(blobWithMd5.getSize())
                                .hash(MD5s.asHex(blobWithMd5.getMd5()))
                                .build());
                    }
                }
            }, sourceBucket).iterate(metadata);
            publish(secondaryAnalysisEvent.build());
            publish(tertiaryAnalysisEvent.build());
            publish(germlineAnalysisEvent.build());
        }
    }

    @NotNull
    public ImmutablePipelineStaged.Builder eventBuilder(final SampleSet set, final Analysis.Type secondary, final String sampleName) {
        return ImmutablePipelineStaged.builder()
                .analysisMolecule(Molecule.DNA)
                .analysisType(secondary)
                .analysisContext(context)
                .version(Versions.pipelineMajorMinorVersion())
                .sample(sampleName)
                .runId(Optional.ofNullable(run.getId()))
                .setId(set.getId());
    }

    private boolean isSecondary(final Blob blobWithMd5) {
        return (stageCrams
                ? InNamespace.of(CramConversion.NAMESPACE)
                : InNamespace.of(Aligner.NAMESPACE)).or(InNamespace.of(BamMetrics.NAMESPACE))
                .or(InNamespace.of(SnpGenotype.NAMESPACE))
                .or(InNamespace.of(Flagstat.NAMESPACE))
                .test(blobWithMd5);
    }

    private boolean isGermline(final Blob blobWithMd5) {
        return InNamespace.of(GermlineCaller.NAMESPACE).test(blobWithMd5);
    }

    private boolean notSecondary(final Blob blobWithMd5) {
        return not(InNamespace.of(CramConversion.NAMESPACE)).and(not(InNamespace.of(Aligner.NAMESPACE))).test(blobWithMd5);
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
