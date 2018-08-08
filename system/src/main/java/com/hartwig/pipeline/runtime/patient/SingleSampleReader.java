package com.hartwig.pipeline.runtime.patient;

import static com.hartwig.pipeline.runtime.patient.Samples.createPairedEndSample;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleSampleReader implements PatientReader {

    private final FileSystem fileSystem;

    SingleSampleReader(final FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Patient read(final Configuration configuration) throws IOException {
        return patientOf(configuration,
                createPairedEndSample(fileSystem, new Path(configuration.patient().directory()), configuration.patient().name(), ""));
    }

    private static Patient patientOf(final Configuration configuration, final Sample single) throws IOException {
        return Patient.of(configuration.patient().directory(), configuration.patient().name(), single);
    }
}
