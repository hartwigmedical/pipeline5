package com.hartwig.pipeline.runtime.patient;

import static com.hartwig.pipeline.runtime.patient.Samples.createPairedEndSample;

import java.io.IOException;
import java.nio.file.Paths;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.configuration.Configuration;

public class SingleSampleReader implements PatientReader {
    @Override
    public Patient read(final Configuration configuration) throws IOException {
        return patientOf(configuration,
                createPairedEndSample(Paths.get(configuration.patient().directory()), configuration.patient().name(), ""));
    }

    private static Patient patientOf(final Configuration configuration, final Sample single) throws IOException {
        return Patient.of(configuration.patient().directory(), configuration.patient().name(), single);
    }
}
