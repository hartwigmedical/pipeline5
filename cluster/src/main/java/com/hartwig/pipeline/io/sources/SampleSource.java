package com.hartwig.pipeline.io.sources;

import com.hartwig.pipeline.Arguments;

public interface SampleSource {

    SampleData sample(Arguments arguments);
}
