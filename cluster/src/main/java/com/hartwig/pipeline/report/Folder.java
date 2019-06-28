package com.hartwig.pipeline.report;

import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

public interface Folder {

    String name();

    static Folder from(SomaticRunMetadata metadata){
        return () -> metadata.reference().sampleName() + "_" + metadata.tumor().sampleName();
    }

    static Folder from(SingleSampleRunMetadata metadata){
        return metadata::sampleName;
    }
}
