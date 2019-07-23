package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;

public interface CompletionHandler {

    default void handleAlignmentComplete(PipelineState state){
        // default
    }

    default void handleSingleSampleComplete(PipelineState state){
        // default
    }
}
