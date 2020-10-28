package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.junit.Before;
import org.junit.Test;

public class BiopsyMetadataApiTest {

    private static final String BIOPSY = "biopsy";
    private SbpRestApi restApi;
    private BiopsyMetadataApi victim;

    @Before
    public void setUp() throws Exception {
        restApi = mock(SbpRestApi.class);
        victim = new BiopsyMetadataApi(restApi, BIOPSY, Arguments.testDefaults());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noSamplesForBiopsy() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void noTumorForBiopsy() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void noReferenceForBiopsy() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.emptyList());
        victim.get();
    }

    @Test
    public void returnsMetadataForBiopsySamples() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.emptyList());
        SomaticRunMetadata somaticRunMetadata = victim.get();
        assertThat(somaticRunMetadata.bucket()).isEqualTo(Arguments.testDefaults().outputBucket());
        assertThat(somaticRunMetadata.name()).isEqualTo("FR11111111-FR22222222");
        assertThat(somaticRunMetadata.tumor().sampleName()).isEqualTo("CPCT12345678T");
        assertThat(somaticRunMetadata.tumor().barcode()).isEqualTo("FR22222222");
        assertThat(somaticRunMetadata.reference().sampleName()).isEqualTo("CPCT12345678R");
        assertThat(somaticRunMetadata.reference().barcode()).isEqualTo("FR11111111");
    }
}