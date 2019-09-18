package com.hartwig.pipeline.metadata;

import java.util.HashSet;
import java.util.Set;

import com.hartwig.pipeline.PipelineState;

public class SingleSampleEventListener {

    private final Set<CompletionHandler> handlers = new HashSet<>();

    public void register(CompletionHandler completionHandler) {
        handlers.add(completionHandler);
    }

    public void alignmentComplete(final PipelineState state) {
        handlers.forEach(handler -> handler.handleAlignmentComplete(state));
    }

    public void complete(PipelineState state) {
        handlers.forEach(handler -> handler.handleSingleSampleComplete(state));
    }

    public Set<CompletionHandler> getHandlers() {
        return handlers;
    }
}
