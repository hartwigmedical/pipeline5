package com.hartwig.pipeline.execution.dataproc;

import java.io.IOException;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface JarUpload {

    JarLocation run(RuntimeBucket runtimeBucket, Arguments arguments) throws IOException;
}
