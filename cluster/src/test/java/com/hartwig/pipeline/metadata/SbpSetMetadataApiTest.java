package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpSetMetadataApiTest {

    private static final int SET_ID = 1;
    private SetMetadataApi victim;
    private SBPRestApi sbpRestApi;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SBPRestApi.class);
        victim = new SbpSetMetadataApi(SET_ID, sbpRestApi);
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() throws Exception {
        when(sbpRestApi.getRun(SET_ID)).thenReturn(TestJson.get("get_run"));
        SetMetadata setMetadata = victim.get();
        assertThat(setMetadata.setName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().type()).isEqualTo(Sample.Type.REFERENCE);
        assertThat(setMetadata.reference().name()).isEqualTo("CPCT02290012R");
        assertThat(setMetadata.tumor().type()).isEqualTo(Sample.Type.TUMOR);
        assertThat(setMetadata.tumor().name()).isEqualTo("CPCT02290012T");
    }

    @Test
    public void mapsSuccessStatusToPipeline5Done() {
        ArgumentCaptor<String> entityType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        victim.complete(PipelineStatus.SUCCESS);
        verify(sbpRestApi, times(1)).updateStatus(entityType.capture(), entityId.capture(), status.capture());
        assertThat(entityType.getValue()).isEqualTo(SBPRestApi.RUNS);
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSetMetadataApi.SNP_CHECK);
    }

    @Test
    public void mapsFailedStatusToPipeline5Finished() {
        ArgumentCaptor<String> entityType = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> entityId = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        victim.complete(PipelineStatus.FAILED);
        verify(sbpRestApi, times(1)).updateStatus(entityType.capture(), entityId.capture(), status.capture());
        assertThat(entityType.getValue()).isEqualTo(SBPRestApi.RUNS);
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(SET_ID));
        assertThat(status.getValue()).isEqualTo(SbpSetMetadataApi.FAILED);
    }
}