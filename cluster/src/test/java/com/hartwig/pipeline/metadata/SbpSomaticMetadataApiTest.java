package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.transfer.sbp.SbpFileTransfer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpSomaticMetadataApiTest {

    private static final int SET_ID = 1;
    private static final String SAMPLE_ID = "7141";
    private SomaticMetadataApi victim;
    private SbpRestApi sbpRestApi;
    private SomaticRunMetadata somaticRunMetadata;
    private SbpFileTransfer sbpFileTransfer;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        sbpFileTransfer = mock(SbpFileTransfer.class);
        somaticRunMetadata = mock(SomaticRunMetadata.class);
        when(sbpRestApi.getInis()).thenReturn(TestJson.get("get_inis"));
        victim = new SbpSomaticMetadataApi(Arguments.testDefaults(), SET_ID, sbpRestApi, sbpFileTransfer);
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set"));
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.runName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().sampleId()).isEqualTo("FR13257296");
        assertThat(setMetadata.reference().entityId()).isEqualTo(49);
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.tumor().sampleId()).isEqualTo("FR13257296");
        assertThat(setMetadata.tumor().entityId()).isEqualTo(50);
    }

    @Test
    public void mapsSuccessStatusToSnpCheck() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        victim.complete(PipelineStatus.SUCCESS, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunStatus(entityId.capture(), status.capture(), any());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSomaticMetadataApi.SNP_CHECK);
    }

    @Test
    public void mapsSuccessStatusToSuccessWhenShallow() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        victim = new SbpSomaticMetadataApi(Arguments.testDefaultsBuilder().shallow(true).build(), SET_ID, sbpRestApi, sbpFileTransfer);
        victim.complete(PipelineStatus.SUCCESS, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunStatus(entityId.capture(), status.capture(), any());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSomaticMetadataApi.SUCCESS);
    }

    @Test
    public void mapsFailedStatusToPipeline5Finished() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        victim.complete(PipelineStatus.FAILED, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunStatus(entityId.capture(), status.capture(), any());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getAllValues().get(1)).isEqualTo(SbpSomaticMetadataApi.FAILED);
    }

    @Test
    public void handlesSingleSampleSet() {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run_single_sample"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.runName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().sampleId()).isEqualTo("FR13257296");
        assertThat(setMetadata.maybeTumor()).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionWhenTumorIsMissing() {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        victim.get();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalStateIfNoBucketInRun() {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run_no_bucket"));
        victim.complete(PipelineStatus.FAILED, somaticRunMetadata);
    }
}
