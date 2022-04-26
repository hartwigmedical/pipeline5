package com.hartwig.pipeline.metadata;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.alignment.AlignmentOutput;

public class SingleSampleEventListener {

    private final List<CompletionHandler> handlers = new ArrayList<>();

    public void register(final CompletionHandler completionHandler) {
        handlers.add(completionHandler);
    }

    public void alignmentComplete(final AlignmentOutput state) {
        handlers.forEach(handler -> handler.handleAlignmentComplete(state));
    }

    public void complete(final PipelineState state) {
        handlers.forEach(handler -> handler.handleSingleSampleComplete(state));
    }

    public List<CompletionHandler> getHandlers() {
        return handlers;
    }
}
