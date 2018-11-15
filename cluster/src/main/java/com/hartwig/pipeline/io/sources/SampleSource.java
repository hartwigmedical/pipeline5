package com.hartwig.pipeline.io.sources;

import com.hartwig.pipeline.bootstrap.Arguments;

public interface SampleSource {

    SampleData sample(Arguments arguments);
}
