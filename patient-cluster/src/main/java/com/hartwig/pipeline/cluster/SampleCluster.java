package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;

public interface SampleCluster {

    void start(Sample sample, Arguments arguments) throws IOException;

    void submit(SparkJobDefinition jobDefinition, Arguments arguments) throws IOException;

    void stop(Arguments arguments) throws IOException;
}
