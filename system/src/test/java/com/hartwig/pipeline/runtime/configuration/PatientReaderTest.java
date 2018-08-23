package com.hartwig.pipeline.runtime.configuration;

import static com.hartwig.testsupport.TestConfigurations.DEFAULT_CONFIG_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.DEFAULT_PATIENT_BUILDER;
import static com.hartwig.testsupport.TestConfigurations.HUNDREDK_READS_HISEQ;
import static com.hartwig.testsupport.TestConfigurations.PATIENT_DIR;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.pipeline.runtime.hadoop.Hadoop;
import com.hartwig.pipeline.runtime.patient.PatientReader;
import com.hartwig.testsupport.Resources;

import org.junit.Test;

public class PatientReaderTest {

    private static final String SINGLE_SAMPLE_DIR = patientDirectory("/singleSample");

    @Test(expected = FileNotFoundException.class)
    public void nonExistentDirectoryThrowsIllegalArgument() throws Exception {
        readerWithDirectory("/not/a/directory");
    }

    @Test
    public void onlyFilesReturnsSingleSampleMode() throws Exception {
        Patient victim = readerWithDirectory(SINGLE_SAMPLE_DIR);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.maybeTumor()).isEmpty();
    }

    @Test
    public void twoCorrectlyNamedSampleDirectoriesReturnReferenceTumorMode() throws Exception {
        Patient victim = readerWithConfiguration(HUNDREDK_READS_HISEQ);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumor()).isNotNull();
    }

    @Test(expected = IllegalStateException.class)
    public void onlyOneSubdirectoryPresentInPatientDirectoryIfNoNameSpecified() throws Exception {
        Configuration configurationNoPatientName =
                DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(patientDirectory("/tooManyPatients")).name("").build())
                        .build();
        readerWithConfiguration(configurationNoPatientName);
    }

    @Test
    public void patientNameTakenFromSubdirectoryIfNoNameSpecified() throws Exception {
        Configuration configurationNoPatientName =
                DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(patientDirectory("/noPatientName")).name("").build())
                        .build();
        Patient victim = readerWithConfiguration(configurationNoPatientName);
        assertThat(victim.name()).isEqualTo("CPCT12345678");
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumor()).isNotNull();
    }

    private static String patientDirectory(final String name) {
        return Resources.testResource(PATIENT_DIR + name);
    }

    private static Patient readerWithDirectory(final String directory) throws IOException {
        Configuration configuration = DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(directory).build()).build();
        return readerWithConfiguration(configuration);
    }

    private static Patient readerWithConfiguration(final Configuration configuration) throws IOException {
        return PatientReader.fromHDFS(Hadoop.fileSystem(configuration), configuration);
    }
}