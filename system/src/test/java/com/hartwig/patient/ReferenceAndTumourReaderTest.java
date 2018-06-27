package com.hartwig.patient;

import static com.hartwig.testsupport.TestConfigurations.DEFAULT_CONFIG_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.Configuration;

import org.junit.Test;

public class ReferenceAndTumourReaderTest {

    private static final Configuration CANCER_PANEL =
            DEFAULT_CONFIG_BUILDER.patientDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/cancerPanel")
                    .patientName("CPCT12345678")
                    .build();
    private static final String CANCER_PANEL_NORMAL_DIRECTORY = CANCER_PANEL.patientDirectory() + "/CPCT12345678R";
    private static final Lane EXPECTED_NORMAL_LANE = Lane.builder()
            .directory(CANCER_PANEL_NORMAL_DIRECTORY)
            .name("CPCT12345678R_L001")
            .readsPath(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz")
            .matesPath(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R2_001.fastq.gz")
            .flowCellId("HJJLGCCXX")
            .suffix("001")
            .index("S1")
            .build();
    private static final String CANCER_PANEL_TUMOUR_DIRECTORY = CANCER_PANEL.patientDirectory() + "/CPCT12345678T";
    private static final Lane EXPECTED_TUMOUR_LANE = Lane.builder()
            .directory(CANCER_PANEL_TUMOUR_DIRECTORY)
            .name("CPCT12345678T_L001")
            .readsPath(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R1_001.fastq.gz")
            .matesPath(System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R2_001.fastq.gz")
            .flowCellId("HJJLGCCXX")
            .suffix("001")
            .index("S1")
            .build();

    @Test
    public void createOutputFromNormalAndTumourDirectory() throws Exception {
        ReferenceAndTumourReader victim = new ReferenceAndTumourReader();
        Patient patient = victim.read(CANCER_PANEL);
        assertThat(patient.directory()).isEqualTo(CANCER_PANEL.patientDirectory());
        assertThat(patient.reference().directory()).isEqualTo(CANCER_PANEL_NORMAL_DIRECTORY);
        assertThat(patient.reference().lanes()).hasSize(1).containsOnly(EXPECTED_NORMAL_LANE);
        assertThat(patient.tumour().directory()).isEqualTo(CANCER_PANEL_TUMOUR_DIRECTORY);
        assertThat(patient.tumour().lanes()).hasSize(1).containsOnly(EXPECTED_TUMOUR_LANE);
    }
}