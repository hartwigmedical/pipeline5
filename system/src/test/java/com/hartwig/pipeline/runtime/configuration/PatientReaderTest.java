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

import org.junit.Test;

public class PatientReaderTest {

    @Test(expected = FileNotFoundException.class)
    public void nonExistentDirectoryThrowsIllegalArgument() throws Exception {
        readerWithDirectory("/not/a/directory");
    }

    @Test
    public void onlyFilesReturnsSingleSampleMode() throws Exception {
        Patient victim = readerWithDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/singleSample");
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.maybeTumour()).isEmpty();
    }

    @Test
    public void twoCorrectlyNamedSampleDirectoriesReturnReferenceTumourMode() throws Exception {
        Patient victim = PatientReader.fromHDFS(Hadoop.fileSystem(HUNDREDK_READS_HISEQ), HUNDREDK_READS_HISEQ);
        assertThat(victim.reference()).isNotNull();
        assertThat(victim.tumour()).isNotNull();
    }

    private static Patient readerWithDirectory(final String directory) throws IOException {
        Configuration configuration = DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(directory).build()).build();
        return PatientReader.fromHDFS(Hadoop.fileSystem(configuration), configuration);
    }

}