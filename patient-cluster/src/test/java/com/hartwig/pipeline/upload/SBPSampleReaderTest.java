package com.hartwig.pipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.support.test.Resources;

import org.junit.Before;
import org.junit.Test;

public class SBPSampleReaderTest {

    private static final int DOESNT_EXIST = 1;
    private static final int EXISTS = 64;

    private static final String SAMPLE_NAME = "CPCT02330029T";
    private static final String SAMPLE_JSON = "sbp_api/get_fastq.json";
    private static final String SAMPLE_JSON_QC_FAILED = "sbp_api/get_fastq_qc_failed.json";
    private SBPRestApi sbpRestApi;
    private SBPSampleReader victim;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SBPRestApi.class);
        victim = new SBPSampleReader(sbpRestApi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentExceptionWhenPatientNotFound() {
        when(sbpRestApi.getFastQ(DOESNT_EXIST)).thenReturn("[]");
        victim.read(DOESNT_EXIST);
    }

    @Test
    public void addsAllLanesToSample() throws Exception {
        returnJson(SAMPLE_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample).isNotNull();
        assertThat(sample.lanes()).hasSize(2);
    }

    @Test
    public void parsesPatientNameFromReadsFile() throws Exception {
        returnJson(SAMPLE_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.name()).isEqualTo(SAMPLE_NAME);
    }

    @Test
    public void filtersLanesWhichHaveNotPassedQC() throws Exception {
        returnJson(SAMPLE_JSON_QC_FAILED);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.lanes()).hasSize(1);
        assertThat(sample.lanes().get(0).readsPath()).contains("L001");
    }

    private void returnJson(final String sampleJsonLocation) throws IOException {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(new String(Files.readAllBytes(Paths.get(Resources.testResource(sampleJsonLocation)))));
    }
}