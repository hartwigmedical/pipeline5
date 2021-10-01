package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.api.SampleApi;
import com.hartwig.api.SetApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleSet;
import com.hartwig.api.model.SampleStatus;
import com.hartwig.api.model.SampleType;
import com.hartwig.events.Analysis.Context;
import com.hartwig.events.Analysis.Molecule;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ResearchMetadataApiTest {

    private static final String BIOPSY = "biopsy";
    private static final String TUMOR_NAME = "tumor";
    private static final String TUMOR_BARCODE = "FR22222222";
    private static final String REF_NAME = "reference";
    private static final String REF_BARCODE = "FR11111111";
    public static final long TUMOR_SAMPLE_ID = 2L;
    public static final String SET_NAME = TestInputs.defaultSomaticRunMetadata().set();
    public static final long SET_ID = 3L;
    public static final long REF_SAMPLE_ID = 4L;
    private ResearchMetadataApi victim;
    private SampleApi sampleApi;
    private SetApi setApi;
    private Bucket bucket;
    private Publisher publisher;

    @Before
    public void setUp() throws Exception {
        sampleApi = mock(SampleApi.class);
        setApi = mock(SetApi.class);
        bucket = mock(Bucket.class);
        publisher = mock(Publisher.class);
        ObjectMapper objectMapper = ObjectMappers.get();
        victim = new ResearchMetadataApi(sampleApi,
                setApi,
                BIOPSY,
                Arguments.testDefaults(),
                new StagedOutputPublisher(setApi, bucket, publisher, objectMapper, new Run(), Context.RESEARCH, false, true),
                new Anonymizer(Arguments.testDefaults()));
    }

    @Test(expected = IllegalStateException.class)
    public void noSamplesForBiopsy() {
        when(sampleApi.list(null, null, null, null, SampleType.TUMOR, BIOPSY)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test(expected = IllegalStateException.class)
    public void noSetForSample() {
        when(sampleApi.list(null, null, null, null, SampleType.TUMOR, BIOPSY)).thenReturn(List.of(tumor()));
        when(setApi.list(null, TUMOR_SAMPLE_ID, true)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test(expected = IllegalStateException.class)
    public void noReferenceSample() {
        when(sampleApi.list(null, null, null, null, SampleType.TUMOR, BIOPSY)).thenReturn(List.of(tumor()));
        when(setApi.list(null, TUMOR_SAMPLE_ID, true)).thenReturn(List.of(new SampleSet().name(SET_NAME).id(SET_ID)));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test
    public void returnsMetadataForBiopsySamples() {
        when(sampleApi.list(null, null, null, null, SampleType.TUMOR, BIOPSY)).thenReturn(List.of(tumor()));
        when(setApi.list(null, TUMOR_SAMPLE_ID, true)).thenReturn(List.of(new SampleSet().name(SET_NAME).id(SET_ID)));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(List.of(ref()));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(List.of(tumor()));
        SomaticRunMetadata somaticRunMetadata = victim.get();
        assertThat(somaticRunMetadata.bucket()).isEqualTo(Arguments.testDefaults().outputBucket());
        assertThat(somaticRunMetadata.name()).isEqualTo(REF_BARCODE + "-" + TUMOR_BARCODE);
        assertThat(somaticRunMetadata.tumor().sampleName()).isEqualTo(TUMOR_NAME);
        assertThat(somaticRunMetadata.tumor().barcode()).isEqualTo(TUMOR_BARCODE);
        assertThat(somaticRunMetadata.reference().sampleName()).isEqualTo(REF_NAME);
        assertThat(somaticRunMetadata.reference().barcode()).isEqualTo(REF_BARCODE);
        assertThat(somaticRunMetadata.tumor().primaryTumorDoids()).containsOnly("1234", "5678");
    }

    @Test
    public void anonymizesSampleNameWhenActivated() {
        victim = new ResearchMetadataApi(sampleApi,
                setApi,
                BIOPSY,
                Arguments.testDefaults(),
                new StagedOutputPublisher(setApi, bucket, publisher, ObjectMappers.get(), new Run(), Context.RESEARCH, true, true),
                new Anonymizer(Arguments.testDefaultsBuilder().anonymize(true).build()));
        when(sampleApi.list(null, null, null, null, SampleType.TUMOR, BIOPSY)).thenReturn(List.of(tumor()));
        when(setApi.list(null, TUMOR_SAMPLE_ID, true)).thenReturn(List.of(new SampleSet().name(SET_NAME).id(SET_ID)));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.REF, null)).thenReturn(List.of(ref()));
        when(sampleApi.list(null, null, null, SET_ID, SampleType.TUMOR, null)).thenReturn(List.of(tumor()));
        SomaticRunMetadata somaticRunMetadata = victim.get();
        assertThat(somaticRunMetadata.tumor().sampleName()).isEqualTo(TUMOR_BARCODE);
        assertThat(somaticRunMetadata.reference().sampleName()).isEqualTo(REF_BARCODE);
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionSomaticFile() throws Exception {
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = pipelineCompleteWithFile("set/purple/tumor.purple.somatic.vcf.gz",
                TestOutput.builder().status(PipelineStatus.SUCCESS).build());

        PipelineStaged result =
                ObjectMappers.get().readValue(pubsubMessageArgumentCaptor.getValue().getData().toByteArray(), PipelineStaged.class);
        assertThat(result.analysisMolecule()).isEqualTo(Molecule.DNA);
        assertThat(result.runId()).isEmpty();
        assertThat(result.setId()).isEqualTo(SET_ID);
        assertThat(result.sample()).isEqualTo("tumor");
        assertThat(result.version()).isEqualTo(Versions.pipelineMajorMinorVersion());
        PipelineOutputBlob blobResult = result.blobs().get(0);
        assertThat(blobResult.barcode()).isEmpty();
        assertThat(blobResult.bucket()).isEqualTo("bucket");
        assertThat(blobResult.datatype()).isEmpty();
        assertThat(blobResult.root()).isEqualTo("set");
        assertThat(blobResult.sampleSubdir()).isEmpty();
        assertThat(blobResult.namespace()).hasValue("purple");
        assertThat(blobResult.filename()).isEqualTo("tumor.purple.somatic.vcf.gz");
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionSingleSampleFile() throws Exception {
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = pipelineCompleteWithFile("set/reference/aligner/reference.bam",
                TestOutput.builder().status(PipelineStatus.SUCCESS).build());

        PipelineStaged result =
                ObjectMappers.get().readValue(pubsubMessageArgumentCaptor.getValue().getData().toByteArray(), PipelineStaged.class);
        PipelineOutputBlob blobResult = result.blobs().get(0);
        assertThat(blobResult.datatype()).isEmpty();
        assertThat(blobResult.root()).isEqualTo("set");
        assertThat(blobResult.sampleSubdir()).hasValue("reference");
        assertThat(blobResult.namespace()).hasValue("aligner");
        assertThat(blobResult.filename()).isEqualTo("reference.bam");
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionRootFile() throws Exception {
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor =
                pipelineCompleteWithFile("set/run.log", TestOutput.builder().status(PipelineStatus.SUCCESS).build());

        PipelineStaged result =
                ObjectMappers.get().readValue(pubsubMessageArgumentCaptor.getValue().getData().toByteArray(), PipelineStaged.class);
        PipelineOutputBlob blobResult = result.blobs().get(0);
        assertThat(blobResult.datatype()).isEmpty();
        assertThat(blobResult.root()).isEqualTo("set");
        assertThat(blobResult.sampleSubdir()).isEmpty();
        assertThat(blobResult.namespace()).isEmpty();
        assertThat(blobResult.filename()).isEqualTo("run.log");
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionWithDataType() throws Exception {
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = pipelineCompleteWithFile("set/purple/tumor.purple.somatic.vcf.gz",
                TestOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                                "tumor",
                                new ArchivePath(Folder.root(), "purple", "tumor.purple.somatic.vcf.gz")))
                        .build());

        PipelineStaged result =
                ObjectMappers.get().readValue(pubsubMessageArgumentCaptor.getValue().getData().toByteArray(), PipelineStaged.class);
        PipelineOutputBlob blobResult = result.blobs().get(0);
        assertThat(blobResult.datatype()).hasValue("SOMATIC_VARIANTS_PURPLE");
    }

    @Test
    public void doesNothingOnFailedPipeline() {
        PipelineState state = new PipelineState();
        state.add(TestOutput.builder().status(PipelineStatus.FAILED).build());
        victim.complete(state, TestInputs.defaultSomaticRunMetadata());
        verify(publisher, never()).publish(any());
    }

    private static Sample tumor() {
        return new Sample().id(TUMOR_SAMPLE_ID)
                .name(TUMOR_NAME)
                .barcode(TUMOR_BARCODE)
                .type(SampleType.TUMOR)
                .status(SampleStatus.READY)
                .primaryTumorDoids(List.of("1234", "5678"));
    }

    private static Sample ref() {
        return new Sample().id(REF_SAMPLE_ID).name(REF_NAME).barcode(REF_BARCODE).type(SampleType.TUMOR).status(SampleStatus.READY);
    }

    @NotNull
    public ArgumentCaptor<PubsubMessage> pipelineCompleteWithFile(final String s, final StageOutput stageOutput) {
        PipelineState state = new PipelineState();
        state.add(stageOutput);
        ArgumentCaptor<PubsubMessage> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        SomaticRunMetadata metadata = TestInputs.defaultSomaticRunMetadata();
        when(setApi.list(metadata.set(), null, true)).thenReturn(List.of(new SampleSet().id(SET_ID)));
        Blob outputBlob = mock(Blob.class);
        when(outputBlob.getBucket()).thenReturn("bucket");
        when(outputBlob.getName()).thenReturn(s);
        when(outputBlob.getSize()).thenReturn(1L);
        when(outputBlob.getMd5()).thenReturn("md5");
        when(bucket.get(s)).thenReturn(outputBlob);
        Page<Blob> page = TestBlobs.pageOf(outputBlob);
        when(bucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        //noinspection unchecked
        when(publisher.publish(pubsubMessageArgumentCaptor.capture())).thenReturn(mock(ApiFuture.class));
        victim.complete(state, metadata);
        return pubsubMessageArgumentCaptor;
    }
}