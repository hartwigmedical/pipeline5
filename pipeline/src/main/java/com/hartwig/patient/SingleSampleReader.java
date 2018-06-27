package com.hartwig.patient;

import static com.hartwig.patient.Samples.createPairedEndSample;

import java.io.IOException;
import java.nio.file.Paths;

import com.hartwig.pipeline.Configuration;

public class SingleSampleReader implements PatientReader {
    @Override
    public Patient read(final Configuration configuration) throws IOException {
        return patientOf(configuration,
                createPairedEndSample(Paths.get(configuration.patientDirectory()), configuration.patientName(), ""));
    }

    private static Patient patientOf(final Configuration configuration, final Sample single) throws IOException {
        return Patient.of(configuration.patientDirectory(), configuration.patientName(), single);
    }
}
