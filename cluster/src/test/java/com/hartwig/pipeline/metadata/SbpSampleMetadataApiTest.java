package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.junit.Before;
import org.junit.Test;

public class SbpSampleMetadataApiTest {

    private static final int SAMPLE_ID = 1;
    private SbpRestApi sbpRestApi;
    private SbpSampleMetadataApi victim;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        victim = new SbpSampleMetadataApi(sbpRestApi, SAMPLE_ID);
    }

    @Test
    public void getsSampleAndSetFromSbpRestApi() {
        when(sbpRestApi.getSample(1)).thenReturn(TestJson.get("get_sample"));
        when(sbpRestApi.getSet(1)).thenReturn(TestJson.get("get_set"));
        SingleSampleRunMetadata sampleMetadata = victim.get();
        assertThat(sampleMetadata.sampleId()).isEqualTo("FR13257296");
        assertThat(sampleMetadata.entityId()).isEqualTo(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentThrownWhenNoSampleForId() {
        when(sbpRestApi.getSample(1)).thenReturn("{\"RESULT\": \"sample not found\"}");
        victim.get();
    }

    public PipelineState failed() {
        PipelineState state = mock(PipelineState.class);
        when(state.status()).thenReturn(PipelineStatus.FAILED);
        return state;
    }
}
