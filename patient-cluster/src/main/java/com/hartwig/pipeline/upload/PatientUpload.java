package com.hartwig.pipeline.upload;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.pipeline.bootstrap.Arguments;

public interface PatientUpload {

    void run(Patient patient, Arguments arguments) throws IOException;

    void cleanup(Patient patient, Arguments arguments) throws IOException;
}
