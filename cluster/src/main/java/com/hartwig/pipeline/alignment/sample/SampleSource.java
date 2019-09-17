package com.hartwig.pipeline.alignment.sample;

import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public interface SampleSource {

    SampleData sample(SingleSampleRunMetadata metadata);
}
