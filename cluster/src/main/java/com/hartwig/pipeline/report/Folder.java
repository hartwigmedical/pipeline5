package com.hartwig.pipeline.report;

import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public interface Folder {

    String name();

    static Folder from() {
        return () -> "";
    }

    static Folder from(SingleSampleRunMetadata metadata) {
        return () -> metadata.sampleName() + "/";
    }
}
