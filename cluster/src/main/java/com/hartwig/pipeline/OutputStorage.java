package com.hartwig.pipeline;

import java.util.concurrent.BlockingQueue;

import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.pipeline.stages.Stage;

public class OutputStorage<S extends StageOutput, M extends RunMetadata> {

    private final BlockingQueue<S> outputQueue;
    private final Arguments arguments;

    public OutputStorage(final BlockingQueue<S> outputQueue, final Arguments arguments) {
        this.outputQueue = outputQueue;
        this.arguments = arguments;
    }

    public S get(final M metadata, final Stage<S, M> stage) {
        if (!stage.shouldRun(arguments)) {
            return stage.skippedOutput(metadata);
        }
        return outputQueue.poll();
    }
}
