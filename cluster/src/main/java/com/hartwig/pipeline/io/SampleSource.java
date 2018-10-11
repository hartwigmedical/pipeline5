package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;

public interface SampleSource {

    Sample sample(Arguments arguments);
}
