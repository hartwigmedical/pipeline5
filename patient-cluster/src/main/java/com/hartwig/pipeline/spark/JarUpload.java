package com.hartwig.pipeline.spark;

import java.io.IOException;

public interface JarUpload {

    JarLocation run(Version version) throws IOException;
}
