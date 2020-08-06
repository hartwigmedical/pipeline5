package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public class ExistingAlignment {

    public static AlignmentOutput find(SingleSampleRunMetadata metadata, AlignmentOutputStorage alignmentOutputStorage) {
        return alignmentOutputStorage.get(metadata)
                .orElseThrow(() -> new IllegalArgumentException(format(
                        "Unable to find output for sample [%s]. Please run the aligner first by setting -run_aligner to true",
                        metadata.sampleName())));
    }
}
