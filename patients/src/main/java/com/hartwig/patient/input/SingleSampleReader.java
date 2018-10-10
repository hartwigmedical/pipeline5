package com.hartwig.patient.input;

import static com.hartwig.patient.input.Samples.createPairedEndSample;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleSampleReader implements PatientReader {

    private final FileSystem fileSystem;

    SingleSampleReader(final FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Patient read(final Path patientPath) throws IOException {
        return patientOf(patientPath, createPairedEndSample(fileSystem, patientPath, patientPath.getName(), ""));
    }

    private static Patient patientOf(final Path patientPath, final Sample single) throws IOException {
        return Patient.of(patientPath.toString(), patientPath.getName(), single);
    }
}
