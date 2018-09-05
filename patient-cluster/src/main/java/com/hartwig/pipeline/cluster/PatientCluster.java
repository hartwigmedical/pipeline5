package com.hartwig.pipeline.cluster;

import java.io.IOException;

public interface PatientCluster {

    void start() throws IOException;

    void submit(SparkJobDefinition jobDefinition) throws IOException;

    void stop() throws IOException;
}
