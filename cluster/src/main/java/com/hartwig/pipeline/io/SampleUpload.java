package com.hartwig.pipeline.io;

import java.io.IOException;

import com.hartwig.patient.Sample;

public interface SampleUpload {

    void run(Sample sample, RuntimeBucket runtimeBucket) throws IOException;
}
