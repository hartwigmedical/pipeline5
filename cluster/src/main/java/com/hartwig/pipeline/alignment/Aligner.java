package com.hartwig.pipeline.alignment;

import com.hartwig.computeengine.input.SingleSampleRunMetadata;

public interface Aligner {

    String NAMESPACE = "aligner";

    AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception;
}
