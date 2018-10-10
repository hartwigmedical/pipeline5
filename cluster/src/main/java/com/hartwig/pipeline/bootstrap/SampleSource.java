package com.hartwig.pipeline.bootstrap;

import com.hartwig.patient.Sample;

public interface SampleSource {

    Sample sample(Arguments arguments);
}
