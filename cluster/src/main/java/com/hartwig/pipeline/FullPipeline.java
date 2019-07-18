package com.hartwig.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FullPipeline {

    private final SingleSamplePipeline referencePipeline;
    private final SingleSamplePipeline tumorPipeline;
    private final SomaticPipeline somaticPipeline;
    private final ExecutorService executorService;

    FullPipeline(final SingleSamplePipeline referencePipeline, final SingleSamplePipeline tumorPipeline,
            final SomaticPipeline somaticPipeline, final ExecutorService executorService) {
        this.referencePipeline = referencePipeline;
        this.tumorPipeline = tumorPipeline;
        this.somaticPipeline = somaticPipeline;
        this.executorService = executorService;
    }

    public PipelineState run() {
        Future<PipelineState> referenceStateFuture = executorService.submit(referencePipeline::run);
        Future<PipelineState> tumorStateFuture = executorService.submit(tumorPipeline::run);
        try {
            PipelineState singleSampleState =
                    new PipelineState().combineWith(referenceStateFuture.get()).combineWith(tumorStateFuture.get());
            if (singleSampleState.shouldProceed()) {
                return singleSampleState.combineWith(somaticPipeline.run());
            } else {
                return singleSampleState;
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
