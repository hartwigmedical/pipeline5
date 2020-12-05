package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.sbpapi.ImmutableSbpSample;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.junit.Before;
import org.junit.Test;

public class BiopsyMetadataApiTest {

    private static final String BIOPSY = "biopsy";
    private static final String TUMOR_NAME = "CPCT12345678T";
    private static final String TUMOR_BARCODE = "FR22222222";
    private static final String REF_NAME = "CPCT12345678R";
    private static final String REF_BARCODE = "FR11111111";
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
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.singletonList(ref()));
        victim.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void noReferenceForBiopsy() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(Collections.singletonList(tumor()));
        victim.get();
    }

    @Test
    public void returnsMetadataForBiopsySamples() {
        when(restApi.getSamplesByBiopsy(BIOPSY)).thenReturn(List.of(tumor(), ref()));
        SomaticRunMetadata somaticRunMetadata = victim.get();
        assertThat(somaticRunMetadata.bucket()).isEqualTo(Arguments.testDefaults().outputBucket());
        assertThat(somaticRunMetadata.name()).isEqualTo(REF_BARCODE + "-" + TUMOR_BARCODE);
        assertThat(somaticRunMetadata.tumor().sampleName()).isEqualTo(TUMOR_NAME);
        assertThat(somaticRunMetadata.tumor().barcode()).isEqualTo(TUMOR_BARCODE);
        assertThat(somaticRunMetadata.reference().sampleName()).isEqualTo(REF_NAME);
        assertThat(somaticRunMetadata.reference().barcode()).isEqualTo(REF_BARCODE);
    }

    private static ImmutableSbpSample tumor() {
        return ImmutableSbpSample.builder().id(1).name(TUMOR_NAME).barcode(TUMOR_BARCODE).type("tumor").status("Ready").build();
    }

    private static ImmutableSbpSample ref() {
        return ImmutableSbpSample.builder().id(1).name(REF_NAME).barcode(REF_BARCODE).type("ref").status("Ready").build();
    }
}