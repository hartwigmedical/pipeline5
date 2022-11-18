package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.SetApi;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleSet;
import com.hartwig.api.model.SampleStatus;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.events.pipeline.Analysis;
import com.hartwig.events.pipeline.Analysis.Molecule;
import com.hartwig.events.pipeline.AnalysisOutputBlob;
import com.hartwig.events.pubsub.EventPublisher;
import com.hartwig.events.pipeline.Pipeline.Context;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.transfer.staged.SetResolver;
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
    private static final long TUMOR_SAMPLE_ID = 2L;
    private static final String SET_NAME = TestInputs.defaultSomaticRunMetadata().set();
    private static final long SET_ID = 3L;
    private static final long REF_SAMPLE_ID = 4L;
    private static final long RUN_ID = 1L;
    private ResearchMetadataApi victim;
    private SampleApi sampleApi;
    private SetApi setApi;
    private RunApi runApi;
    private Run run;
    private Bucket bucket;
    private EventPublisher<PipelineComplete> publisher;
    private SetResolver setResolver;

    @Before
    public void setUp() throws Exception {
        sampleApi = mock(SampleApi.class);
        setApi = mock(SetApi.class);
        bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn("bucket");
        runApi = mock(RunApi.class);
        run = new Run().id(RUN_ID).version("5.28.6");
        publisher = mock(EventPublisher.class);
        setResolver = mock(SetResolver.class);
        victim = new ResearchMetadataApi(sampleApi,
                setApi,
                runApi,
                Optional.of(run),
                BIOPSY,
                Arguments.testDefaults(),
                new StagedOutputPublisher(setResolver, bucket, publisher, Optional.of(run), Context.RESEARCH, false, true),
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
        assertThat(somaticRunMetadata.runName()).isEqualTo(REF_BARCODE + "-" + TUMOR_BARCODE);
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
                runApi,
                Optional.of(run),
                BIOPSY,
                Arguments.testDefaults(),
                new StagedOutputPublisher(mock(SetResolver.class), bucket, publisher, Optional.of(run), Context.RESEARCH, true, true),
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
    public void publishesPipelineStagedEventOnCompletion() throws Exception {
        PipelineComplete result = pipelineCompleteWithFile("set/purple/tumor.purple.somatic.vcf.gz",
                TestOutput.builder().status(PipelineStatus.SUCCESS).build());
        assertThat(result.pipeline().runId()).isEqualTo(1);
        assertThat(result.pipeline().setId()).isEqualTo(SET_ID);
        assertThat(result.pipeline().sample()).isEqualTo("tumor");
        assertThat(result.pipeline().version()).isEqualTo(run.getVersion());

        Analysis analysis = result.pipeline().analyses().get(1);
        assertThat(analysis.molecule()).isEqualTo(Molecule.DNA);
        AnalysisOutputBlob blobResult = analysis.output().get(0);
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
        PipelineComplete result = pipelineCompleteWithFile("set/reference/aligner/reference.bam",
                TestOutput.builder().status(PipelineStatus.SUCCESS).build());
        Analysis analysis = result.pipeline().analyses().get(0);
        assertThat(analysis.molecule()).isEqualTo(Molecule.DNA);
        AnalysisOutputBlob blobResult = analysis.output().get(0);
        assertThat(blobResult.datatype()).isEmpty();
        assertThat(blobResult.root()).isEqualTo("set");
        assertThat(blobResult.sampleSubdir()).hasValue("reference");
        assertThat(blobResult.namespace()).hasValue("aligner");
        assertThat(blobResult.filename()).isEqualTo("reference.bam");
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionRootFile() {
        PipelineComplete result = pipelineCompleteWithFile("set/run.log", TestOutput.builder().status(PipelineStatus.SUCCESS).build());
        Analysis analysis = result.pipeline().analyses().get(1);
        assertThat(analysis.molecule()).isEqualTo(Molecule.DNA);
        AnalysisOutputBlob blobResult = analysis.output().get(0);
        assertThat(blobResult.datatype()).isEmpty();
        assertThat(blobResult.root()).isEqualTo("set");
        assertThat(blobResult.sampleSubdir()).isEmpty();
        assertThat(blobResult.namespace()).isEmpty();
        assertThat(blobResult.filename()).isEqualTo("run.log");
    }

    @Test
    public void publishesPipelineStagedEventOnCompletionWithDataType() throws Exception {
        PipelineComplete result = pipelineCompleteWithFile("set/purple/tumor.purple.somatic.vcf.gz",
                TestOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                                "tumor",
                                new ArchivePath(Folder.root(), "purple", "tumor.purple.somatic.vcf.gz")))
                        .build());
        AnalysisOutputBlob blobResult = result.pipeline().analyses().get(1).output().get(0);
        assertThat(blobResult.datatype()).hasValue("SOMATIC_VARIANTS_PURPLE");
    }

    @Test
    public void doesNothingOnFailedPipeline() {
        PipelineState state = new PipelineState();
        state.add(TestOutput.builder().status(PipelineStatus.FAILED).build());
        victim.complete(state, TestInputs.defaultSomaticRunMetadata());
        verify(publisher, never()).publish(any());
    }

    @Test
    public void setsStatusAndStartTimeOnStart() {
        ArgumentCaptor<Long> runIdArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<UpdateRun> updateRunArgumentCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        when(runApi.update(runIdArgumentCaptor.capture(), updateRunArgumentCaptor.capture())).thenReturn(run);
        victim.start();
        assertThat(runIdArgumentCaptor.getValue()).isEqualTo(RUN_ID);
        UpdateRun updateRun = updateRunArgumentCaptor.getValue();
        assertThat(updateRun.getStartTime()).isNotNull();
        assertThat(updateRun.getStatus()).isEqualTo(Status.PROCESSING);
    }

    @Test
    public void setsStatusAndEndTimeOnComplete() {
        ArgumentCaptor<Long> runIdArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<UpdateRun> updateRunArgumentCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        when(runApi.update(runIdArgumentCaptor.capture(), updateRunArgumentCaptor.capture())).thenReturn(run);
        PipelineState state = new PipelineState();
        state.add(TestOutput.builder().status(PipelineStatus.FAILED).build());
        victim.complete(state, TestInputs.defaultSomaticRunMetadata());
        assertThat(runIdArgumentCaptor.getValue()).isEqualTo(RUN_ID);
        UpdateRun updateRun = updateRunArgumentCaptor.getValue();
        assertThat(updateRun.getEndTime()).isNotNull();
        assertThat(updateRun.getStatus()).isEqualTo(Status.FAILED);
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
    public PipelineComplete pipelineCompleteWithFile(final String s, final StageOutput stageOutput) {
        PipelineState state = new PipelineState();
        state.add(stageOutput);
        ArgumentCaptor<PipelineComplete> pubsubMessageArgumentCaptor = ArgumentCaptor.forClass(PipelineComplete.class);
        SomaticRunMetadata metadata = TestInputs.defaultSomaticRunMetadata();
        when(setResolver.resolve(metadata.set(), true)).thenReturn(new SampleSet().id(SET_ID));
        Blob outputBlob = mock(Blob.class);
        when(outputBlob.getBucket()).thenReturn("bucket");
        when(outputBlob.getName()).thenReturn(s);
        when(outputBlob.getSize()).thenReturn(1L);
        when(outputBlob.getMd5()).thenReturn("md5");
        when(bucket.get(s)).thenReturn(outputBlob);
        Page<Blob> page = TestBlobs.pageOf(outputBlob);
        when(bucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        victim.complete(state, metadata);
        verify(publisher).publish(pubsubMessageArgumentCaptor.capture());
        return pubsubMessageArgumentCaptor.getValue();
    }
}