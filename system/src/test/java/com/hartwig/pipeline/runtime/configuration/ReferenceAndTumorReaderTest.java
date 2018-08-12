package com.hartwig.pipeline.runtime.configuration;

import static com.hartwig.testsupport.TestConfigurations.DEFAULT_CONFIG_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.DEFAULT_PATIENT_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.pipeline.runtime.hadoop.Hadoop;
import com.hartwig.pipeline.runtime.patient.ReferenceAndTumorReader;

import org.junit.Test;

public class ReferenceAndTumorReaderTest {

    private static final Configuration CANCER_PANEL =
            DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(System.getProperty("user.dir") + PATIENT_DIR + "/cancerPanel")
                    .name("CPCT12345678")
                    .build()).build();
    private static final String CANCER_PANEL_REFERENCE_DIRECTORY = "file:" + CANCER_PANEL.patient().directory() + "/CPCT12345678R";
    private static final Lane EXPECTED_REFERENCE_LANE = Lane.builder().directory(CANCER_PANEL_REFERENCE_DIRECTORY)
            .name("CPCT12345678R_L001")
            .readsPath("file:" + System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R1_001.fastq.gz")
            .matesPath("file:" + System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678R/"
                    + "CPCT12345678R_HJJLGCCXX_S1_L001_R2_001.fastq.gz")
            .flowCellId("HJJLGCCXX")
            .suffix("001")
            .index("S1")
            .build();
    private static final String CANCER_PANEL_TUMOR_DIRECTORY = "file:" + CANCER_PANEL.patient().directory() + "/CPCT12345678T";
    private static final Lane EXPECTED_TUMOR_LANE = Lane.builder().directory(CANCER_PANEL_TUMOR_DIRECTORY)
            .name("CPCT12345678T_L001")
            .readsPath("file:" + System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R1_001.fastq.gz")
            .matesPath("file:" + System.getProperty("user.dir") + "/src/test/resources/patients/cancerPanel/CPCT12345678T/"
                    + "CPCT12345678T_HJJLGCCXX_S1_L001_R2_001.fastq.gz")
            .flowCellId("HJJLGCCXX")
            .suffix("001")
            .index("S1")
            .build();

    @Test
    public void createOutputFromReferenceAndTumorDirectory() throws Exception {
        ReferenceAndTumorReader victim = new ReferenceAndTumorReader(Hadoop.fileSystem(CANCER_PANEL));
        Patient patient = victim.read(CANCER_PANEL);
        assertThat(patient.directory()).isEqualTo(CANCER_PANEL.patient().directory());
        assertThat(patient.reference().directory()).isEqualTo(CANCER_PANEL_REFERENCE_DIRECTORY);
        assertThat(patient.reference().lanes()).hasSize(1).containsOnly(EXPECTED_REFERENCE_LANE);
        assertThat(patient.tumor().directory()).isEqualTo(CANCER_PANEL_TUMOR_DIRECTORY);
        assertThat(patient.tumor().lanes()).hasSize(1).containsOnly(EXPECTED_TUMOR_LANE);
    }
}