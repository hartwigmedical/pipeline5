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
import java.util.Optional;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.SampleSet;
import com.hartwig.events.pipeline.Analysis;
import com.hartwig.events.pipeline.AnalysisOutputBlob;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pubsub.EventPublisher;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StagedOutputPublisherTest {

    private Bucket bucket;
    private EventPublisher<PipelineComplete> publisher;
    private PipelineState state;
    private StagedOutputPublisher victim;
    private SetResolver setResolver;
    private Run run;

    @Before
    public void setUp() throws Exception {
        bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn("bucket");
        publisher = mock(EventPublisher.class);
        state = mock(PipelineState.class);
        setResolver = mock(SetResolver.class);
        run = new Run().ini(Ini.SOMATIC_INI.getValue()).id(1L).version("5.28.6");
        victim = new StagedOutputPublisher(setResolver, bucket, publisher, Optional.of(run), Pipeline.Context.DIAGNOSTIC, true, false);
    }

    @Test
    public void doesNothingOnFailedState() {
        when(state.status()).thenReturn(PipelineStatus.FAILED);
        victim.publish(state, TestInputs.defaultSomaticRunMetadata());
        verify(publisher, never()).publish(any());
    }

    @Test
    public void publishesDnaSecondaryAnalysisOnBam() throws Exception {
        victim = new StagedOutputPublisher(setResolver, bucket, publisher, Optional.of(run), Pipeline.Context.DIAGNOSTIC, false, false);
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
        PipelineComplete result = publish(page, TestInputs.defaultSomaticRunMetadata());
        assertThat(result.pipeline().analyses().get(1).type()).isEqualTo(Analysis.Type.SOMATIC);
        assertThat(result.pipeline().analyses().get(1).output()).extracting(AnalysisOutputBlob::filename)
                .containsExactlyInAnyOrder("tumor.vcf");
    }

    @Test
    public void publishesGermlineAnalysisOnGatkGermlineVcf() throws Exception {
        verifyGermline(".germline.vcf.gz", "reference.germline.vcf.gz", "/germline_caller/");
    }

    @Test
    public void publishesGermlineAnalysisOnGatkGermlineVcfIndex() throws Exception {
        verifyGermline(".germline.vcf.gz.tbi", "reference.germline.vcf.gz.tbi", "/germline_caller/");
    }

    @Test
    public void publishesGermlineAnalysisOnSageGermlineVcf() throws Exception {
        verifyGermline(".germline.vcf.gz", "reference.germline.vcf.gz", "/sage_germline/");
    }

    @Test
    public void publishesGermlineAnalysisOnPurpleGermlineVcf() throws Exception {
        verifyGermline(".germline.vcf.gz", "reference.germline.vcf.gz", "/purple/");
    }

    @Test
    public void usesReferenceSampleWhenNoTumor() {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob("/germline_caller/" + TestInputs.referenceSample() + "reference.germline.vcf.gz"));
        Page<Blob> page = pageOf(vcf);
        PipelineComplete result = publish(page, TestInputs.defaultSingleSampleRunMetadata());
        assertThat(result.pipeline().sample()).isEqualTo("reference");
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
        PipelineComplete result = publish(page, TestInputs.defaultSomaticRunMetadata());
        assertThat(result.pipeline().analyses().get(2).output().get(0).datatype()).hasValue("GERMLINE_VARIANTS");
        assertThat(result.pipeline().analyses().get(2).output().get(0).barcode()).hasValue("barcode");
    }

    public void verifyGermline(final String filename, final String expectedFile, final String namespace) {
        when(state.status()).thenReturn(PipelineStatus.SUCCESS);
        Blob vcf = withBucketAndMd5(blob(namespace + TestInputs.referenceSample() + filename));
        Page<Blob> page = pageOf(vcf);
        PipelineComplete result = publish(page, TestInputs.defaultSomaticRunMetadata());
        assertThat(result.pipeline().analyses().get(2).output()).extracting(AnalysisOutputBlob::filename)
                .containsExactlyInAnyOrder(expectedFile);
    }

    private void verifySecondaryAnalysis(final String extension, final String indexExtension, final String namespace) {
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
        PipelineComplete result = publish(page, TestInputs.defaultSomaticRunMetadata());
        assertThat(result.pipeline().context()).isEqualTo(Pipeline.Context.DIAGNOSTIC);
        assertThat(result.pipeline().analyses().get(0).type()).isEqualTo(Analysis.Type.ALIGNMENT);
        assertThat(result.pipeline().analyses().get(0).output()).hasSize(4);
        assertThat(result.pipeline().analyses().get(0).output()).extracting(AnalysisOutputBlob::filename)
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

    private PipelineComplete publish(final Page<Blob> page, final SomaticRunMetadata metadata) {
        when(bucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        ArgumentCaptor<PipelineComplete> eventArgumentCaptor = ArgumentCaptor.forClass(PipelineComplete.class);
        when(setResolver.resolve(metadata.set(), false)).thenReturn(new SampleSet().id(1L));
        victim.publish(state, metadata);
        verify(publisher).publish(eventArgumentCaptor.capture());
        return eventArgumentCaptor.getValue();
    }

}