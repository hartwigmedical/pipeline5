package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRunFailure;
import com.hartwig.pipeline.sbpapi.SbpRunResultUpdate;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ClinicalSomaticMetadataApiTest {

    private static final SbpRunFailure FAILED = SbpRunFailure.from(PipelineStatus.FAILED);

    private static final int RUN_ID = 1;
    private static final String SAMPLE_ID = "7141";
    private SomaticMetadataApi victim;
    private SbpRestApi sbpRestApi;
    private SomaticRunMetadata somaticRunMetadata;
    private PipelineState pipelineState;
    private ArgumentCaptor<String> entityId;
    private ArgumentCaptor<SbpRunResultUpdate> status;
    private StagedOutputPublisher publisher;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        somaticRunMetadata = TestInputs.defaultSomaticRunMetadata();
        when(sbpRestApi.getInis()).thenReturn(TestJson.get("get_inis"));
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run"));
        entityId = ArgumentCaptor.forClass(String.class);
        status = ArgumentCaptor.forClass(SbpRunResultUpdate.class);
        pipelineState = mock(PipelineState.class);
        when(pipelineState.status()).thenReturn(PipelineStatus.SUCCESS);
        publisher = mock(StagedOutputPublisher.class);
        victim = new ClinicalSomaticMetadataApi(Arguments.testDefaults(), RUN_ID, sbpRestApi, publisher);
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() {
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.reference().entityId()).isEqualTo(49);
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.tumor().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.tumor().entityId()).isEqualTo(50);
        assertThat(setMetadata.tumor().primaryTumorDoids()).containsOnly("1234", "5678");
    }

    @Test
    public void mapsSuccessStatusToSnpCheck() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(1)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(ClinicalSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().failure()).isEmpty();
    }

    @Test
    public void mapsSuccessStatusToSuccessWhenShallow() {
        victim = new ClinicalSomaticMetadataApi(Arguments.testDefaultsBuilder().shallow(true).build(), RUN_ID, sbpRestApi, publisher);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(1)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(ClinicalSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().failure()).isEmpty();
    }

    @Test
    public void mapsFailedStatusToPipeline5Failed() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(1)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(ClinicalSomaticMetadataApi.FAILED);
        assertThat(status.getValue().failure()).contains(FAILED);
    }

    @Test
    public void handlesSingleSampleSet() {
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run_single_sample"));
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.maybeTumor()).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionWhenTumorIsMissing() {
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        victim.get();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalStateIfNoBucketInRun() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run_no_bucket"));
        victim.complete(pipelineState, somaticRunMetadata);
    }

    @Test
    public void setsStatusToProcessingAndResultNullOnStartup() {
        ArgumentCaptor<String> runIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SbpRunResultUpdate> runResultUpdateCaptor = ArgumentCaptor.forClass(SbpRunResultUpdate.class);
        victim.start();
        verify(sbpRestApi).updateRunResult(runIdCaptor.capture(), runResultUpdateCaptor.capture());
        assertThat(runIdCaptor.getValue()).isEqualTo(String.valueOf(RUN_ID));
        SbpRunResultUpdate udpate = runResultUpdateCaptor.getValue();
        assertThat(udpate.failure()).isEmpty();
        assertThat(udpate.status()).isEqualTo(ClinicalSomaticMetadataApi.PROCESSING);
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
}
