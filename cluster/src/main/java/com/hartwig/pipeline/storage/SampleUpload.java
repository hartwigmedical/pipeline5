package com.hartwig.pipeline.storage;

import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pdl.SampleInput;

import java.io.IOException;

public interface SampleUpload {

    void run(SampleInput sample, RuntimeBucket runtimeBucket) throws IOException;
}
