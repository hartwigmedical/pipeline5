package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.pipeline.bootstrap.Arguments;

public interface PatientCluster {

    void start(Patient patient, Arguments arguments) throws IOException;

    void submit(SparkJobDefinition jobDefinition, Arguments arguments) throws IOException;

    void stop(Arguments arguments) throws IOException;
}
