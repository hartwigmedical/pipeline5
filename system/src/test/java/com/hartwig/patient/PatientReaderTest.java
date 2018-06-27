package com.hartwig.patient;

import static com.hartwig.testsupport.TestConfigurations.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.testsupport.TestConfigurations;

import org.junit.Test;

public class PatientReaderTest {

    @Test(expected = IllegalArgumentException.class)
    public void nonExistentDirectoryThrowsIllegalArgument() throws Exception {
        PatientReader.from(TestConfigurations.DEFAULT_CONFIG_BUILDER.patientDirectory("/not/a/directory").build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyDirectoryThrowsIllegalArgument() throws Exception {
        PatientReader.from(TestConfigurations.DEFAULT_CONFIG_BUILDER.patientDirectory(
                System.getProperty("user.dir") + PATIENT_DIR + "/empty").build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void incorrectlyNamedSubdirectoriesThrowsIllegalArgument() throws Exception {
        PatientReader.from(TestConfigurations.DEFAULT_CONFIG_BUILDER.patientDirectory(
                System.getProperty("user.dir") + PATIENT_DIR + "/incorrectlyNamed").build());
    }

    @Test
    public void onlyFilesReturnsSingleSampleMode() throws Exception {
        Patient victim = PatientReader.from(TestConfigurations.DEFAULT_CONFIG_BUILDER.patientDirectory(
                System.getProperty("user.dir") + PATIENT_DIR + "/singleSample").build());
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.maybeTumour()).isEmpty();
    }

    @Test
    public void twoCorrectlyNamedSampleDirectoriesReturnReferenceTumourMode() throws Exception {
        Patient victim = PatientReader.from(TestConfigurations.HUNDREDK_READS_HISEQ);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumour()).isNotNull();
    }
}