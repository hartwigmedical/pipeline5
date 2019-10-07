package com.hartwig.pipeline.alignment.sample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metadata.TestJson;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.junit.Before;
import org.junit.Test;

public class SbpSampleReaderTest {

    private static final int DOESNT_EXIST = 1;
    private static final int EXISTS = 64;

    private static final String SAMPLE_NAME = "CPCT02330029T";
    private static final String FASTQ_JSON = "get_fastq";
    private static final String FASTQ_JSON_SINGLE_QC_FAILED = "get_fastq_qc_failed";
    private static final String FASTQ_JSON_ALL_QC_FAILED = "get_fastq_all_qc_failed";
    private static final String FASTQ_JSON_SUBDIRECTORIES = "get_fastq_subdirectories";
    private static final String SAMPLE_JSON = "get_sample";
    private static final String BAD_FASTQ_NAME = "get_fastq_bad_filename";
    private SbpRestApi sbpRestApi;
    private SbpSampleReader victim;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        victim = new SbpSampleReader(sbpRestApi);
        when(sbpRestApi.getSample(EXISTS)).thenReturn(TestJson.get(SAMPLE_JSON));
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
    public void getsFlowcellAndIndexFromFileName()  throws Exception {
        returnJson(FASTQ_JSON);
        Sample sample = victim.read(EXISTS);
        assertThat(sample.lanes().get(0).flowCellId()).isEqualTo("HJKLMALXX");
        assertThat(sample.lanes().get(0).index()).isEqualTo("S6");
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
    public void throwsIllegalArgumentWhenAllLanesFilteredQc() throws Exception {
        returnJson(FASTQ_JSON_ALL_QC_FAILED);
        victim.read(EXISTS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentWhenFastqNameIncorrect() throws Exception {
        returnJson(BAD_FASTQ_NAME);
        victim.read(EXISTS);
    }

    private void returnJson(final String sampleJsonLocation) throws IOException {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(TestJson.get(sampleJsonLocation));
    }
}
