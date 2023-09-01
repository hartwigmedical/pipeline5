package com.hartwig.pipeline.storage;

import java.io.IOException;

import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pdl.SampleInput;

public interface SampleUpload {

    void run(SampleInput sample, RuntimeBucket runtimeBucket) throws IOException;
}
