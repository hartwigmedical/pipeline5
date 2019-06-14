package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.sbp.SBPRestApi;
import com.hartwig.support.test.Resources;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class SbpSetMetadataApiTest {

    private static final int SET_ID = 1;

    @Test
    public void retrievesSetMetadataFromSbpRestApi() throws Exception {
        SBPRestApi sbpRestApi = mock(SBPRestApi.class);
        SetMetadataApi setMetadataApi = new SbpSetMetadataApi(SET_ID, sbpRestApi);
        when(sbpRestApi.getRun(SET_ID)).thenReturn(testJson());
        SetMetadata setMetadata = setMetadataApi.get();
        assertThat(setMetadata.setName()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().type()).isEqualTo(Sample.Type.REFERENCE);
        assertThat(setMetadata.reference().name()).isEqualTo("CPCT02290012R");
        assertThat(setMetadata.tumor().type()).isEqualTo(Sample.Type.TUMOR);
        assertThat(setMetadata.tumor().name()).isEqualTo("CPCT02290012T");
    }

    @NotNull
    private String testJson() throws IOException {
        return new String(Files.readAllBytes(Paths.get(Resources.testResource("sbp_api/get_run.json"))));
    }
}