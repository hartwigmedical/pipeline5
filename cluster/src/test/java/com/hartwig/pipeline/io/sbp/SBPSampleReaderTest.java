package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.support.test.Resources;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class SBPSampleReaderTest {

    private static final int DOESNT_EXIST = 1;
    private static final int EXISTS = 64;

    private static final String SAMPLE_NAME = "CPCT02330029T";
    private static final String FASTQ_JSON = "sbp_api/get_fastq.json";
    private static final String FASTQ_JSON_SINGLE_QC_FAILED = "sbp_api/get_fastq_qc_failed.json";
    private static final String FASTQ_JSON_ALL_QC_FAILED = "sbp_api/get_fastq_all_qc_failed.json";
    private static final String FASTQ_JSON_SUBDIRECTORIES = "sbp_api/get_fastq_subdirectories.json";
    private static final String SAMPLE_JSON = "sbp_api/get_sample.json";
    private SBPRestApi sbpRestApi;
    private SBPSampleReader victim;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SBPRestApi.class);
        victim = new SBPSampleReader(sbpRestApi);
        when(sbpRestApi.getSample(EXISTS)).thenReturn(testJson(SAMPLE_JSON));
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentExceptionWhenPatientNotFound() {
        when(sbpRestApi.getFastQ(DOESNT_EXIST)).thenReturn("[]");
        victim.read(DOESNT_EXIST);
    }

    @Test
    public void addsAllLanesToSample() throws Exception {
        returnJson(FASTQ_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample).isNotNull();
        assertThat(sample.lanes()).hasSize(2);
    }

    @Test
    public void parsesPatientNameFromReadsFile() throws Exception {
        returnJson(FASTQ_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.name()).isEqualTo(SAMPLE_NAME);
    }

    @Test
    public void getsBarcodeFromAPi() throws Exception {
        returnJson(FASTQ_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.barcode()).isEqualTo("FR13257296");
    }

    @Test
    public void filtersLanesWhichHaveNotPassedQC() throws Exception {
        returnJson(FASTQ_JSON_SINGLE_QC_FAILED);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.lanes()).hasSize(1);
        assertThat(sample.lanes().get(0).firstOfPairPath()).contains("L001");
    }

    @Test
    public void handlesSubdirectories() throws Exception {
        returnJson(FASTQ_JSON_SUBDIRECTORIES);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.lanes()).hasSize(2);
        assertThat(sample.name()).isEqualTo("CPCT02330029T");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentWhenAllLanesFilteredQc() throws Exception{
        returnJson(FASTQ_JSON_ALL_QC_FAILED);
        Sample sample = victim.read(EXISTS);
    }

    private void returnJson(final String sampleJsonLocation) throws IOException {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(testJson(sampleJsonLocation));
    }

    @NotNull
    private String testJson(final String sampleJsonLocation) throws IOException {
        return new String(Files.readAllBytes(Paths.get(Resources.testResource(sampleJsonLocation))));
    }
}