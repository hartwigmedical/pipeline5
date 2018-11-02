package com.hartwig.pipeline.io.sources;

import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;

public interface SampleSource {

    SampleData sample(Arguments arguments, RuntimeBucket runtimeBucket);
}
