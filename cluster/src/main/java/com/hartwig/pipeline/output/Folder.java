package com.hartwig.pipeline.output;

import com.hartwig.computeengine.input.SingleSampleRunMetadata;

public interface Folder {

    String name();

    static Folder root() {
        return () -> "";
    }

    static Folder from(final SingleSampleRunMetadata metadata) {
        return () -> metadata.sampleName() + "/";
    }
}
