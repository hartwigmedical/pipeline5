package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.pipeline.bootstrap.Arguments;

public interface JarUpload {

    JarLocation run(Arguments arguments) throws IOException;
}
