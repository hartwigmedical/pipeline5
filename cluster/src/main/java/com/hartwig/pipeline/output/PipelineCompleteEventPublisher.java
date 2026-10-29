package com.hartwig.pipeline.output;

import static java.util.function.Predicate.not;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.pipeline.Analysis;
import com.hartwig.events.pipeline.AnalysisOutputBlob;
import com.hartwig.events.pipeline.ImmutableAnalysis;
import com.hartwig.events.pipeline.ImmutableAnalysisOutputBlob;
import com.hartwig.events.pipeline.ImmutablePipeline;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.tools.VersionUtils;

import org.jetbrains.annotations.NotNull;

public class PipelineCompleteEventPublisher implements OutputPublisher {
    private static final String FASTQ_FOLDER = "fastq";

    private final Bucket sourceBucket;
    private final EventPublisher<PipelineComplete> publisher;
    private final Pipeline.Context context;
    private final boolean stageCrams;

    public PipelineCompleteEventPublisher(final Bucket sourceBucket, final EventPublisher<PipelineComplete> publisher,
            final Pipeline.Context context, final boolean stageCrams) {
        this.sourceBucket = sourceBucket;
        this.publisher = publisher;
        this.context = context;
        this.stageCrams = stageCrams;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static AnalysisOutputBlob createBlob(final Optional<String> tumorSample, final Optional<String> referenceSample,
            final Optional<AddDatatype> dataType, final Blob blobWithHash) {
        return builderWithPathComponents(tumorSample.orElse(""), referenceSample.orElse(""), blobWithHash.getName()).datatype(dataType.map(
                        AddDatatype::dataType).map(Object::toString))
                .barcode(dataType.map(AddDatatype::barcode))
                .bucket(blobWithHash.getBucket())
                .filesize(blobWithHash.getSize())
                .hash(hash(blobWithHash))
                .build();
    }

    private static ImmutableAnalysisOutputBlob.Builder builderWithPathComponents(final String tumorSample, final String refSample,
            final String blobName) {
        ImmutableAnalysisOutputBlob.Builder outputBlob = AnalysisOutputBlob.builder();
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

    public void publish(final PipelineState state, final SomaticRunMetadata metadata) {
        if (state.status() != PipelineStatus.FAILED && metadata.maybeExternalIds().isPresent()) {
            List<AddDatatype> addDatatypes =
                    state.stageOutputs().stream().map(StageOutput::datatypes).flatMap(List::stream).collect(Collectors.toList());
            Optional<String> tumorSampleName = metadata.maybeTumor().map(SingleSampleRunMetadata::sampleName);
            Optional<String> refSampleName = metadata.maybeReference().map(SingleSampleRunMetadata::sampleName);
            ImmutableAnalysis.Builder alignedReadsAnalysis = eventBuilder(Analysis.Type.ALIGNMENT);
            ImmutableAnalysis.Builder somaticAnalysis = eventBuilder(Analysis.Type.SOMATIC);
            ImmutableAnalysis.Builder germlineAnalysis = eventBuilder(Analysis.Type.GERMLINE);

            OutputDataset outputDataset = new OutputDataset(sourceBucket, metadata.set());
            OutputIterator.from(blob -> {
                if (isInputFastq(blob, metadata)) {
                    return;
                }
                Optional<AddDatatype> dataType = addDatatypes.stream().filter(d -> blob.getName().endsWith(d.path())).findFirst();
                dataType.ifPresent(d -> outputDataset.add(d, blob));
                Blob blobWithHash = sourceBucket.get(blob.getName());
                if (isSecondary(blobWithHash)) {
                    alignedReadsAnalysis.addOutput(createBlob(tumorSampleName, refSampleName, dataType, blobWithHash));
                } else {
                    if (isGermline(blobWithHash)) {
                        germlineAnalysis.addOutput(createBlob(tumorSampleName, refSampleName, dataType, blobWithHash));
                    } else if (notSecondary(blobWithHash)) {
                        somaticAnalysis.addOutput(createBlob(tumorSampleName, refSampleName, dataType, blobWithHash));
                    }
                }
            }, sourceBucket).iterate(metadata);
            outputDataset.serializeAndUpload();
            publish(PipelineComplete.builder()
                    .pipeline(ImmutablePipeline.builder()
                            .engine(Pipeline.Engine.PIPELINE5)
                            .molecule(Pipeline.Molecule.DNA)
                            .sample(tumorSampleName.orElseGet(refSampleName::orElseThrow))
                            .bucket(sourceBucket.getName())
                            .rootPath(Optional.of(metadata.set()))
                            .runId(metadata.maybeExternalIds().get().runId())
                            .setId(metadata.maybeExternalIds().get().setId())
                            .context(context)
                            .addAnalyses(alignedReadsAnalysis.build(), somaticAnalysis.build(), germlineAnalysis.build())
                            .version(VersionUtils.pipelineMajorMinorVersion())
                            .build())
                    .build());
        }
    }

    @NotNull
    private ImmutableAnalysis.Builder eventBuilder(final Analysis.Type secondary) {
        return ImmutableAnalysis.builder().molecule(Analysis.Molecule.DNA).type(secondary);
    }

    // The research pipeline launcher copies the input FASTQs into <set>/fastq/ so they are cleaned up with the run. They are not
    // pipeline output.
    private static boolean isInputFastq(final Blob blob, final SomaticRunMetadata metadata) {
        return blob.getName().startsWith(metadata.set() + "/" + FASTQ_FOLDER + "/");
    }

    // Composite objects (e.g. FASTQs uploaded with parallel composite uploads) have no MD5, only a CRC32C
    private static String hash(final Blob blob) {
        return blob.getMd5() != null ? MD5s.asHex(blob.getMd5()) : blob.getCrc32cToHexString();
    }

    private boolean isSecondary(final Blob blobWithHash) {
        return (stageCrams
                ? InNamespace.of(CramConversion.NAMESPACE)
                : InNamespace.of(Aligner.NAMESPACE)).or(InNamespace.of(BamMetrics.NAMESPACE))
                .or(InNamespace.of(SnpGenotype.NAMESPACE))
                .test(blobWithHash);
    }

    private boolean isGermline(final Blob blobWithHash) {
        return InNamespace.of(GermlineCaller.NAMESPACE)
                .or(InNamespace.of(SageConfiguration.SAGE_GERMLINE_NAMESPACE))
                .or(b -> b.getName().contains("germline") && !b.getName().contains("no_germline"))
                .test(blobWithHash);
    }

    private boolean notSecondary(final Blob blobWithHash) {
        return not(InNamespace.of(CramConversion.NAMESPACE)).and(not(InNamespace.of(Aligner.NAMESPACE))).test(blobWithHash);
    }

    public void publish(final PipelineComplete event) {
        if (event.pipeline().analyses().stream().map(Analysis::output).mapToLong(List::size).sum() > 0) {
            publisher.publish(event);
        }
    }
}
