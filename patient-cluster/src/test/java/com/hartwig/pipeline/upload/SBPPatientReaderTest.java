package com.hartwig.pipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import com.hartwig.patient.Patient;
import com.hartwig.support.test.Resources;

import org.junit.Before;
import org.junit.Test;

public class SBPPatientReaderTest {

    private static final String DOESNT_EXIST = "doesnt_exist";
    private static final String EXISTS = "CPCT02330029";
    private static final String REF_ONLY = "sbp_api/get_fastq_reference_only.json";
    private static final String REF_AND_TUMOR = "sbp_api/get_fastq_reference_and_tumor.json";
    private static final String TUMOR_ONLY = "sbp_api/get_fastq_tumor_only.json";
    private SBPRestApi sbpRestApi;
    private SBPPatientReader victim;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SBPRestApi.class);
        victim = new SBPPatientReader(sbpRestApi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentExceptionWhenPatientNotFound() {
        when(sbpRestApi.getFastQ(DOESNT_EXIST)).thenReturn(Optional.empty());
        victim.read(DOESNT_EXIST);
    }

    @Test
    public void addsAllMatchingLanesToPatient() throws Exception {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(Optional.of(new String(Files.readAllBytes(Paths.get(Resources.testResource(REF_ONLY))))));
        Patient patient = victim.read(EXISTS);
        assertThat(patient.reference()).isNotNull();
        assertThat(patient.reference().lanes()).hasSize(2);
    }

    @Test
    public void readsTumourIfPresent() throws Exception {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(Optional.of(new String(Files.readAllBytes(Paths.get(Resources.testResource(
                REF_AND_TUMOR))))));
        Patient patient = victim.read(EXISTS);
        assertThat(patient.maybeTumor()).isPresent();
        assertThat(patient.tumor().lanes()).hasSize(2);
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateWhenOnlyTumor() throws Exception {
        when(sbpRestApi.getFastQ(EXISTS)).thenReturn(Optional.of(new String(Files.readAllBytes(Paths.get(Resources.testResource(TUMOR_ONLY))))));
        victim.read(EXISTS);
    }
}