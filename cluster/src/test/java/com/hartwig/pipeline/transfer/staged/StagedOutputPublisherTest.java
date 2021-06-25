package com.hartwig.pipeline.transfer.staged;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;
import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.api.SetApi;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.SampleSet;
import com.hartwig.events.Analysis.Context;
import com.hartwig.events.Analysis.Type;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StagedOutputPublisherTest {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.get();
    private SetApi setApi;
    private Bucket bucket;
    private Publisher publisher;
    private PipelineState state;
    private StagedOutputPublisher victim;

    @Before
    public void setUp() throws Exception {
        setApi = mock(SetApi.class);
        bucket = mock(Bucket.class);
        publisher = mock(Publisher.class);
        state = mock(PipelineState.class);
        victim = new StagedOutputPublisher(setApi,
                bucket,
                publisher,
                OBJECT_MAPPER,
                new Run().ini(Ini.SOMATIC_INI.getValue()),
                Context.DIAGNOSTIC,
                true,
                false);
    }

    @Test
    public void doesNothingOnFailedState() {
        when(state.status()).thenReturn(PipelineStatus.FAILED);
        victim.publish(state, TestInputs.defaultSomaticRunMetadata());
        verify(publisher, never()).publish(any());
    }

    @Test
    public void publishesDnaSecondaryAnalysisOnBam() throws Exception {
        victim = new StagedOutputPublisher(setApi,
                bucket,
                publisher,
                OBJECT_MAPPER,
                new Run().ini(Ini.SOMATIC_INI.getValue()),
                Context.DIAGNOSTIC,
                false,
                false);
        verifySecondaryAnalysis("bam", "bai", "aligner");
    }

    @Test
    public void publishesDnaSecondaryAnalysisOnCram() throws Exception {
        verifySecondaryAnalysis("cram", "crai", "cram");
    }

    @Test
    public void publishesDnaTertiaryAnalysisOnVcf() throws Exception {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob("/sage/" + TestInputs.tumorSample() + ".vcf"));
        Page<Blob> page = pageOf(vcf);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = publish(page, TestInputs.defaultSomaticRunMetadata());

        PipelineStaged result =
                OBJECT_MAPPER.readValue(new String(messageArgumentCaptor.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.analysisType()).isEqualTo(Type.TERTIARY);
        assertThat(result.blobs()).extracting(PipelineOutputBlob::filename).containsExactlyInAnyOrder("tumor.vcf");
    }

    @Test
    public void publishesGermlineTertiaryAnalysisOnGermlineVcf() throws Exception {
        verifyGermline(".germline.vcf.gz", "reference.germline.vcf.gz");
    }

    @Test
    public void usesReferenceSampleWhenNoTumor() throws Exception {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob("/germline_caller/" + TestInputs.referenceSample() + "reference.germline.vcf.gz"));
        Page<Blob> page = pageOf(vcf);
        ArgumentCaptor<PubsubMessage> published = publish(page, TestInputs.defaultSingleSampleRunMetadata());
        PipelineStaged result = OBJECT_MAPPER.readValue(new String(published.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.sample()).isEqualTo("reference");
    }

    @Test
    public void publishesGermlineTertiaryAnalysisOnGermlineVcfIndex() throws Exception {
        verifyGermline(".germline.vcf.gz.tbi", "reference.germline.vcf.gz.tbi");
    }

    @Test
    public void usesDatatypeAndBarcodeWhenFileMatched() throws Exception {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        String path = TestInputs.referenceSample() + "/germline_caller/reference.germline.vcf.gz";
        Blob vcf = withBucketAndMd5(blob(path));
        Page<Blob> page = pageOf(vcf);
        StageOutput stageOutput = mock(StageOutput.class);
        when(stageOutput.datatypes()).thenReturn(List.of(new AddDatatype(DataType.GERMLINE_VARIANTS,
                "barcode",
                new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), "germline_caller", "reference.germline.vcf.gz"))));
        when(state.stageOutputs()).thenReturn(List.of(stageOutput));
        ArgumentCaptor<PubsubMessage> published = publish(page, TestInputs.defaultSingleSampleRunMetadata());
        PipelineStaged result = OBJECT_MAPPER.readValue(new String(published.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.blobs().get(0).datatype()).hasValue("GERMLINE_VARIANTS");
        assertThat(result.blobs().get(0).barcode()).hasValue("barcode");
    }

    public void verifyGermline(final String filename, final String expectedFile) throws com.fasterxml.jackson.core.JsonProcessingException {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob("/germline_caller/" + TestInputs.referenceSample() + filename));
        Page<Blob> page = pageOf(vcf);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = publish(page, TestInputs.defaultSomaticRunMetadata());

        PipelineStaged result =
                OBJECT_MAPPER.readValue(new String(messageArgumentCaptor.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.analysisType()).isEqualTo(Type.GERMLINE);
        assertThat(result.blobs()).extracting(PipelineOutputBlob::filename).containsExactlyInAnyOrder(expectedFile);
    }

    private void verifySecondaryAnalysis(final String extension, final String indexExtension, final String namespace)
            throws com.fasterxml.jackson.core.JsonProcessingException {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob tumorBamBlob =
                withBucketAndMd5(blob(TestInputs.tumorSample() + "/" + namespace + "/" + TestInputs.tumorSample() + "." + extension));
        Blob tumorBaiBlob = withBucketAndMd5(blob(
                TestInputs.tumorSample() + "/" + namespace + "/" + TestInputs.tumorSample() + "." + extension + "." + indexExtension));
        Blob refBamBlob =
                withBucketAndMd5(blob(TestInputs.tumorSample() + "/" + namespace + "/" + TestInputs.referenceSample() + "." + extension));
        Blob refBaiBlob = withBucketAndMd5(blob(
                TestInputs.tumorSample() + "/" + namespace + "/" + TestInputs.referenceSample() + "." + extension + "." + indexExtension));
        Page<Blob> page = pageOf(tumorBamBlob, tumorBaiBlob, refBamBlob, refBaiBlob);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = publish(page, TestInputs.defaultSomaticRunMetadata());

        PipelineStaged result =
                OBJECT_MAPPER.readValue(new String(messageArgumentCaptor.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.analysisContext()).isEqualTo(Context.DIAGNOSTIC);
        assertThat(result.analysisType()).isEqualTo(Type.SECONDARY);
        assertThat(result.blobs()).hasSize(4);
        assertThat(result.blobs()).extracting(PipelineOutputBlob::filename)
                .containsExactlyInAnyOrder(TestInputs.tumorSample() + "." + extension,
                        TestInputs.tumorSample() + "." + extension + "." + indexExtension,
                        TestInputs.referenceSample() + "." + extension,
                        TestInputs.referenceSample() + "." + extension + "." + indexExtension);
    }

    private Blob withBucketAndMd5(final Blob blob) {
        when(blob.getBucket()).thenReturn("bucket");
        when(blob.getMd5()).thenReturn("md5");
        when(bucket.get(blob.getName())).thenReturn(blob);
        return blob;
    }

    private ArgumentCaptor<PubsubMessage> publish(final Page<Blob> page, final SomaticRunMetadata metadata) {
        when(bucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        when(setApi.list(metadata.set(), null, null)).thenReturn(List.of(new SampleSet().id(1L)));
        //noinspection unchecked
        when(publisher.publish(messageArgumentCaptor.capture())).thenReturn(mock(ApiFuture.class));
        victim.publish(state, metadata);
        return messageArgumentCaptor;
    }

}