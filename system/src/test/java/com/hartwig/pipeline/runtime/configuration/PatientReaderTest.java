package com.hartwig.pipeline.runtime.configuration;

import static com.hartwig.testsupport.TestConfigurations.DEFAULT_CONFIG_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.DEFAULT_PATIENT_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.pipeline.runtime.patient.PatientReader;

import org.junit.Test;

public class PatientReaderTest {

    @Test(expected = IllegalArgumentException.class)
    public void nonExistentDirectoryThrowsIllegalArgument() throws Exception {
        readerWithDirectory("/not/a/directory");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyDirectoryThrowsIllegalArgument() throws Exception {
        readerWithDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/empty");
    }

    @Test(expected = IllegalArgumentException.class)
    public void incorrectlyNamedSubdirectoriesThrowsIllegalArgument() throws Exception {
        readerWithDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/incorrectlyNamed");
    }

    @Test
    public void onlyFilesReturnsSingleSampleMode() throws Exception {
        Patient victim = readerWithDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/singleSample");
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.maybeTumour()).isEmpty();
    }

    @Test
    public void twoCorrectlyNamedSampleDirectoriesReturnReferenceTumourMode() throws Exception {
        Patient victim = PatientReader.from(HUNDREDK_READS_HISEQ);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumour()).isNotNull();
    }

    private static Patient readerWithDirectory(final String directory) throws IOException {
        return PatientReader.from(DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(directory).build()).build());
    }

}