package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;

public interface JarUpload {

    JarLocation run(RuntimeBucket runtimeBucket, Arguments arguments) throws IOException;
}
