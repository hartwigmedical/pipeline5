package com.hartwig.samples;

import static com.hartwig.testsupport.TestPatients.DEFAULT_CONFIG_BUILDER;
import static com.hartwig.testsupport.TestPatients.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Lane;
import com.hartwig.patient.RawSequencingOutput;
import com.hartwig.pipeline.Configuration;

import org.junit.Test;

public class RawSequencingOutputTest {

    private static final Configuration CANCER_PANEL =
            DEFAULT_CONFIG_BUILDER.patientDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/cancerPanel")
                    .patientName("CPCT12345678")
                    .build();
    private static final Configuration CANCER_PANEL_SINGLE_DIRECTORY =
            DEFAULT_CONFIG_BUILDER.patientDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/cancerPanelSingleDirectory")
                    .patientName("CPCT12345678")
                    .build();
    private static final String CANCER_PANEL_NORMAL_DIRECTORY = CANCER_PANEL.patientDirectory() + "/CPCT12345678R";
    private static final Lane EXPECTED_NORMAL_LANE = Lane.of(CANCER_PANEL_NORMAL_DIRECTORY,
            "CPCT12345678R_L001",
            System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz",
            System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R2_001.fastq.gz");
    private static final String CANCER_PANEL_TUMOUR_DIRECTORY = CANCER_PANEL.patientDirectory() + "/CPCT12345678T";
    private static final Lane EXPECTED_TUMOUR_LANE = Lane.of(CANCER_PANEL_TUMOUR_DIRECTORY,
            "CPCT12345678T_L001",
            System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R1_001.fastq.gz",
            System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R2_001.fastq.gz");

    @Test
    public void createOutputFromNormalAndTumourDirectory() throws Exception {
        RawSequencingOutput victim = RawSequencingOutput.from(CANCER_PANEL);
        assertThat(victim.patient().directory()).isEqualTo(CANCER_PANEL.patientDirectory());
        assertThat(victim.patient().reference().directory()).isEqualTo(CANCER_PANEL_NORMAL_DIRECTORY);
        assertThat(victim.patient().reference().lanes()).hasSize(1).containsOnly(EXPECTED_NORMAL_LANE);
        assertThat(victim.patient().tumour().directory()).isEqualTo(CANCER_PANEL_TUMOUR_DIRECTORY);
        assertThat(victim.patient().tumour().lanes()).hasSize(1).containsOnly(EXPECTED_TUMOUR_LANE);
    }

    @Test
    public void createOutputFromNormalAndTumourInSameDirectory() throws Exception {
        RawSequencingOutput victim = RawSequencingOutput.from(CANCER_PANEL_SINGLE_DIRECTORY);
        assertThat(victim.patient().directory()).isEqualTo(CANCER_PANEL_SINGLE_DIRECTORY.patientDirectory());
        assertThat(victim.patient().reference().directory()).isEqualTo(CANCER_PANEL_SINGLE_DIRECTORY.patientDirectory());
        assertThat(victim.patient().reference().lanes()).hasSize(1);
        assertThat(victim.patient().tumour().directory()).isEqualTo(CANCER_PANEL_SINGLE_DIRECTORY.patientDirectory());
        assertThat(victim.patient().tumour().lanes()).hasSize(1);
    }
}