package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.transfer.SbpFileTransfer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpSomaticMetadataApiTest {

    private static final int SET_ID = 1;
    private SomaticMetadataApi victim;
    private SbpRestApi sbpRestApi;
    private SbpFileTransfer sbpFileTransfer;
    private SomaticRunMetadata somaticRunMetadata;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        sbpFileTransfer = mock(SbpFileTransfer.class);
        somaticRunMetadata = mock(SomaticRunMetadata.class);
        victim = new SbpSomaticMetadataApi(Arguments.testDefaults(),
                SET_ID,
                sbpRestApi, sbpFileTransfer,
                LocalDateTime.of(2019, 7, 1, 0, 0));
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() throws Exception {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.runName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().sampleId()).isEqualTo("FR13257296");
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.tumor().sampleId()).isEqualTo("FR13257296");
    }

    @Test
    public void mapsSuccessStatusToPipeline5Done() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        victim.complete(PipelineStatus.SUCCESS, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunStatus(entityId.capture(), status.capture(), any());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSomaticMetadataApi.SNP_CHECK);
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
    public void usesBucketFromRunIdIfExists() {
        ArgumentCaptor<String> bucket = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run_custom_bucket"));
        victim.complete(PipelineStatus.FAILED, somaticRunMetadata);
        verify(sbpFileTransfer).publish(eq(somaticRunMetadata), any(), bucket.capture());
        assertThat(bucket.getValue()).isEqualTo("custom");
    }

    @Test
    public void createdWeeklyBucketIfNoBucketInRun() {
        ArgumentCaptor<String> bucket = ArgumentCaptor.forClass(String.class);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        victim.complete(PipelineStatus.FAILED, somaticRunMetadata);
        verify(sbpFileTransfer).publish(eq(somaticRunMetadata), any(), bucket.capture());
        assertThat(bucket.getValue()).isEqualTo("hmf-output-2019-27");
    }
}
