package com.hartwig.pipeline.alignment.sample;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public interface SampleSource {

    Sample sample(SingleSampleRunMetadata metadata);
}
