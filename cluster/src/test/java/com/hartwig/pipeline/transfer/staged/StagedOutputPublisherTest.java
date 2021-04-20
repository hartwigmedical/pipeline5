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
import com.hartwig.api.model.SampleSet;
import com.hartwig.events.PipelineOutputBlob;
import com.hartwig.events.PipelineStaged;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
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
        victim = new StagedOutputPublisher(setApi, bucket, publisher, OBJECT_MAPPER, null);
    }

    @Test
    public void doesNothingOnFailedState() {
        when(state.status()).thenReturn(PipelineStatus.FAILED);
        victim.publish(state, TestInputs.defaultSomaticRunMetadata());
        verify(publisher, never()).publish(any());
    }

    @Test
    public void publishesDnaSecondaryAnalysisOnBam() throws Exception {
        verifySecondaryAnalysis("bam", "bai");
    }

    @Test
    public void publishesDnaSecondaryAnalysisOnCram() throws Exception {
        verifySecondaryAnalysis("cram", "crai");
    }

    @Test
    public void publishesDnaTertiaryAnalysisOnVcf() throws Exception {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob("/sage/" + TestInputs.tumorSample() + ".vcf"));
        Page<Blob> page = pageOf(vcf);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = publish(page);

        PipelineStaged result =
                OBJECT_MAPPER.readValue(new String(messageArgumentCaptor.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.analysis()).isEqualTo(PipelineStaged.Analysis.TERTIARY);
        assertThat(result.blobs()).extracting(PipelineOutputBlob::filename).containsExactlyInAnyOrder("tumor.vcf");
    }

    public void verifySecondaryAnalysis(final String extension, final String indexExtension)
            throws com.fasterxml.jackson.core.JsonProcessingException {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob tumorBamBlob = withBucketAndMd5(blob(TestInputs.tumorSample() + "/aligner/" + TestInputs.tumorSample() + "." + extension));
        Blob tumorBaiBlob = withBucketAndMd5(blob(
                TestInputs.tumorSample() + "/aligner/" + TestInputs.tumorSample() + "." + extension + "." + indexExtension));
        Blob refBamBlob = withBucketAndMd5(blob(TestInputs.tumorSample() + "/aligner/" + TestInputs.referenceSample() + "." + extension));
        Blob refBaiBlob = withBucketAndMd5(blob(
                TestInputs.tumorSample() + "/aligner/" + TestInputs.referenceSample() + "." + extension + "." + indexExtension));
        Page<Blob> page = pageOf(tumorBamBlob, tumorBaiBlob, refBamBlob, refBaiBlob);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = publish(page);

        PipelineStaged result =
                OBJECT_MAPPER.readValue(new String(messageArgumentCaptor.getValue().getData().toByteArray()), PipelineStaged.class);
        assertThat(result.analysis()).isEqualTo(PipelineStaged.Analysis.SECONDARY);
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

    private ArgumentCaptor<PubsubMessage> publish(final Page<Blob> page) {
        when(bucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        ArgumentCaptor<PubsubMessage> messageArgumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        SomaticRunMetadata metadata = TestInputs.defaultSomaticRunMetadata();
        when(setApi.list(metadata.set(), null, true)).thenReturn(List.of(new SampleSet().id(1L)));
        //noinspection unchecked
        when(publisher.publish(messageArgumentCaptor.capture())).thenReturn(mock(ApiFuture.class));
        victim.publish(state, metadata);
        return messageArgumentCaptor;
    }

}