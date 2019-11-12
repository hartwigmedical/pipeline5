package com.hartwig.pipeline.metadata;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.PipelineState;

public class SingleSampleEventListener {

    private final List<CompletionHandler> handlers = new ArrayList<>();

    public void register(CompletionHandler completionHandler) {
        handlers.add(completionHandler);
    }

    public void alignmentComplete(final PipelineState state) {
        handlers.forEach(handler -> handler.handleAlignmentComplete(state));
    }

    public void complete(PipelineState state) {
        handlers.forEach(handler -> handler.handleSingleSampleComplete(state));
    }

    public List<CompletionHandler> getHandlers() {
        return handlers;
    }
}
