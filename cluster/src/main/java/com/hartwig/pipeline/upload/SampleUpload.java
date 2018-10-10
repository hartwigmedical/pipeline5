package com.hartwig.pipeline.upload;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

public interface SampleUpload {

    void run(Sample sample, RuntimeBucket runtimeBucket) throws IOException;
}
