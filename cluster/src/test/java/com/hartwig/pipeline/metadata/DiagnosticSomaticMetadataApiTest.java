package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class DiagnosticSomaticMetadataApiTest {

    private static final long RUN_ID = 1;
    private static final long SET_ID = 2;
    private static final long TUMOR_SAMPLE_ID = 3;
    private static final long REF_SAMPLE_ID = 4;
    private static final Run SOMATIC_RUN = new Run().id(RUN_ID).bucket("bucket").set(new RunSet().id(SET_ID).name("set"));
    private static final Sample REF = new Sample().type(SampleType.REF).name("ref").barcode("ref_barcode").id(REF_SAMPLE_ID);
    private static final Sample TUMOR = new Sample().type(SampleType.TUMOR)
            .name("tumor")
            .barcode("tumor_barcode")
            .id(TUMOR_SAMPLE_ID)
            .primaryTumorDoids(List.of("1234", "5678"));
    private SomaticMetadataApi victim;
    private SomaticRunMetadata somaticRunMetadata;
    private PipelineState pipelineState;
    private ArgumentCaptor<Long> runIdCaptor;
    private ArgumentCaptor<UpdateRun> updateCaptor;
    private StagedOutputPublisher publisher;
    private RunApi runApi;
    private SampleApi sampleApi;

    @Before
    public void setUp() throws Exception {
        somaticRunMetadata = TestInputs.defaultSomaticRunMetadata();
        runIdCaptor = ArgumentCaptor.forClass(Long.class);
        updateCaptor = ArgumentCaptor.forClass(UpdateRun.class);
        pipelineState = mock(PipelineState.class);
        when(pipelineState.status()).thenReturn(PipelineStatus.SUCCESS);
        publisher = mock(StagedOutputPublisher.class);
        runApi = mock(RunApi.class);
        sampleApi = mock(SampleApi.class);
        victim = new DiagnosticSomaticMetadataApi(SOMATIC_RUN, runApi, sampleApi, publisher, new Anonymizer(Arguments.testDefaults()));
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() {
        when(sampleApi.list(null, null, null, SET_ID, null, null)).thenReturn(List.of(REF, TUMOR));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("set");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ref");
        assertThat(setMetadata.reference().barcode()).isEqualTo("ref_barcode");
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("tumor");
        assertThat(setMetadata.tumor().barcode()).isEqualTo("tumor_barcode");
        assertThat(setMetadata.tumor().primaryTumorDoids()).containsOnly("1234", "5678");
    }

    @Test
    public void anonymizesSampleNameWhenActivated() {
        victim = new DiagnosticSomaticMetadataApi(SOMATIC_RUN,
                runApi,
                sampleApi,
                publisher,
                new Anonymizer(Arguments.testDefaultsBuilder().anonymize(true).build()));
        when(sampleApi.list(null, null, null, SET_ID, null, null)).thenReturn(List.of(REF, TUMOR));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ref_barcode");
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("tumor_barcode");
    }

    @Test
    public void mapsSuccessStatusToFinished() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(runApi).update(runIdCaptor.capture(), updateCaptor.capture());
        assertThat(runIdCaptor.getValue()).isEqualTo(RUN_ID);
        assertThat(updateCaptor.getValue().getStatus()).isEqualTo(Status.FINISHED);
        assertThat(updateCaptor.getValue().getFailure()).isNull();
    }

    @Test
    public void mapsFailedStatusToPipeline5Failed() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(runApi).update(runIdCaptor.capture(), updateCaptor.capture());
        assertThat(runIdCaptor.getValue()).isEqualTo(RUN_ID);
        assertThat(updateCaptor.getValue().getStatus()).isEqualTo(Status.FAILED);
        assertThat(updateCaptor.getValue().getFailure()).isEqualTo(new RunFailure().type(RunFailure.TypeEnum.TECHNICALFAILURE)
                .source("Pipeline"));
    }

    @Test
    public void handlesTumorOnly() {
        when(sampleApi.list(null, null, null, SET_ID, null, null)).thenReturn(List.of(TUMOR));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("set");
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("tumor");
        assertThat(setMetadata.tumor().barcode()).isEqualTo("tumor_barcode");
        assertThat(setMetadata.maybeReference()).isEmpty();
    }

    @Test
    public void handlesReferenceOnly() {
        when(sampleApi.list(null, null, null, SET_ID, null, null)).thenReturn(List.of(REF));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("set");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ref");
        assertThat(setMetadata.reference().barcode()).isEqualTo("ref_barcode");
        assertThat(setMetadata.maybeTumor()).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalStateIfNoBucketInRun() {
        victim = new DiagnosticSomaticMetadataApi(new Run().bucket(null),
                runApi,
                sampleApi,
                publisher,
                new Anonymizer(Arguments.testDefaults()));
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
    }

    @Test
    public void setsStatusToProcessingResultNullAndStartTimeOnStartup() {
        victim.start();
        verify(runApi).update(runIdCaptor.capture(), updateCaptor.capture());
        assertThat(runIdCaptor.getValue()).isEqualTo(RUN_ID);
        UpdateRun update = updateCaptor.getValue();
        assertThat(update.getFailure()).isNull();
        assertThat(update.getStatus()).isEqualTo(Status.PROCESSING);
        assertThat(update.getStartTime()).isNotNull();
    }

    @Test
    public void publishesStagedEventForSuccessfulRuns() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(publisher, times(1)).publish(pipelineState, somaticRunMetadata);
    }

    @Test
    public void doesNotPublishStagedEventForFailedRuns() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(publisher, never()).publish(pipelineState, somaticRunMetadata);
    }

    @Test
    public void setsEndTimeOnCompletion() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(runApi).update(runIdCaptor.capture(), updateCaptor.capture());
        UpdateRun update = updateCaptor.getValue();
        assertThat(update.getEndTime()).isNotNull();
    }
}
