package com.hartwig.pipeline.upload;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;

public interface SampleUpload {

    void run(Sample sample, Arguments arguments) throws IOException;

    void cleanup(Sample sample, Arguments arguments) throws IOException;
}
