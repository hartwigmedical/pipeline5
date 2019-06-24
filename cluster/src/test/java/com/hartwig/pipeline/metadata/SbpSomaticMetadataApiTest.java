package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpSomaticMetadataApiTest {

    private static final int SET_ID = 1;
    private SomaticMetadataApi victim;
    private SBPRestApi sbpRestApi;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SBPRestApi.class);
        victim = new SbpSomaticMetadataApi(Arguments.testDefaults(), SET_ID, sbpRestApi);
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() throws Exception {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set"));
        when(sbpRestApi.getSample("7141")).thenReturn(TestJson.get("get_samples_by_set"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.runName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012-7048");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().sampleId()).isEqualTo("FR13257296");
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.tumor().sampleId()).isEqualTo("FR13257296");
    }

    @Test
    public void mapsSuccessStatusToPipeline5Done() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        victim.complete(PipelineStatus.SUCCESS);
        verify(sbpRestApi, times(1)).updateRunStatus(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSomaticMetadataApi.SNP_CHECK);
    }

    @Test
    public void mapsFailedStatusToPipeline5Finished() {
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        victim.complete(PipelineStatus.FAILED);
        verify(sbpRestApi, times(1)).updateRunStatus(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSomaticMetadataApi.FAILED);
    }
}