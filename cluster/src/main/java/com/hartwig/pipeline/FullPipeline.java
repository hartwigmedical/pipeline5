package com.hartwig.pipeline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.metadata.CompletionHandler;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(FullPipeline.class);

    private final SingleSamplePipeline referencePipeline;
    private final SingleSamplePipeline tumorPipeline;
    private final SomaticPipeline somaticPipeline;
    private final ExecutorService executorService;
    private final SingleSampleEventListener referenceSampleEventListener;
    private final SingleSampleEventListener tumorSampleEventListener;
    private final SomaticMetadataApi api;
    private final SomaticRunMetadata metadata;
    private final Cleanup cleanup;

    FullPipeline(final SingleSamplePipeline referencePipeline, final SingleSamplePipeline tumorPipeline,
            final SomaticPipeline somaticPipeline, final ExecutorService executorService, final SingleSampleEventListener referenceApi,
            final SingleSampleEventListener tumorApi, final SomaticMetadataApi api, final Cleanup cleanup) {
        this.referencePipeline = referencePipeline;
        this.tumorPipeline = tumorPipeline;
        this.somaticPipeline = somaticPipeline;
        this.executorService = executorService;
        this.referenceSampleEventListener = referenceApi;
        this.tumorSampleEventListener = tumorApi;
        this.api = api;
        this.metadata = api.get();
        this.cleanup = cleanup;
    }

    public PipelineState run() {

        final CountDownLatch bothSingleSamplesAlignmentComplete = new CountDownLatch(2);
        final CountDownLatch bothSingleSamplesPipelineComplete = new CountDownLatch(2);

        CountDownAndTrapStatus trapReferenceAlignmentComplete = new CountDownAndTrapStatus(bothSingleSamplesAlignmentComplete, true);
        CountDownAndTrapStatus trapTumorAlignmentComplete = new CountDownAndTrapStatus(bothSingleSamplesAlignmentComplete, true);
        CountDownAndTrapStatus trapReferencePipelineComplete = new CountDownAndTrapStatus(bothSingleSamplesPipelineComplete, false);
        CountDownAndTrapStatus trapTumorPipelineComplete = new CountDownAndTrapStatus(bothSingleSamplesPipelineComplete, false);
        referenceSampleEventListener.register(trapReferenceAlignmentComplete);
        referenceSampleEventListener.register(trapReferencePipelineComplete);
        tumorSampleEventListener.register(trapTumorAlignmentComplete);
        tumorSampleEventListener.register(trapTumorPipelineComplete);
        executorService.submit(() -> metadata.maybeReference()
                .map(reference -> runPipeline(referencePipeline, reference, bothSingleSamplesAlignmentComplete))
                .orElseGet(countdown(bothSingleSamplesAlignmentComplete, bothSingleSamplesPipelineComplete)));
        executorService.submit(() -> metadata.maybeTumor()
                .map(tumor -> runPipeline(tumorPipeline, tumor, bothSingleSamplesAlignmentComplete))
                .orElseGet(countdown(bothSingleSamplesAlignmentComplete, bothSingleSamplesPipelineComplete)));
        waitForSingleSamples(bothSingleSamplesAlignmentComplete);

        PipelineState singleSampleAlignmentState = combine(trapReferenceAlignmentComplete, trapTumorAlignmentComplete, metadata);

        if (singleSampleAlignmentState.shouldProceed()) {
            PipelineState somaticState = runPipeline(trapReferenceAlignmentComplete, trapTumorAlignmentComplete);
            waitForSingleSamples(bothSingleSamplesPipelineComplete);
            PipelineState singleSamplePipelineState = combine(trapReferencePipelineComplete, trapTumorPipelineComplete, metadata);
            PipelineState combinedState = singleSampleAlignmentState.combineWith(somaticState).combineWith(singleSamplePipelineState);
            api.complete(combinedState, metadata);
            if (combinedState.shouldProceed()) {
                cleanup.run(metadata);
            }
            return combinedState;
        } else {
            return singleSampleAlignmentState;
        }
    }

    @NotNull
    private PipelineState runPipeline(final CountDownAndTrapStatus trapReferenceAlignmentComplete,
            final CountDownAndTrapStatus trapTumorAlignmentComplete) {
        return somaticPipeline.run(AlignmentPair.builder()
                .maybeReference(metadata.maybeReference().map(r -> trapReferenceAlignmentComplete.trappedAlignmentOutput))
                .maybeTumor(metadata.maybeTumor().map(r -> trapTumorAlignmentComplete.trappedAlignmentOutput))
                .build());
    }

    private static Supplier<PipelineState> countdown(final CountDownLatch bothSingleSamplesAlignmentComplete,
            final CountDownLatch bothSingleSamplesPipelineComplete) {
        return () -> {
            bothSingleSamplesAlignmentComplete.countDown();
            bothSingleSamplesPipelineComplete.countDown();
            return empty();
        };
    }

    private PipelineState runPipeline(SingleSamplePipeline pipeline, SingleSampleRunMetadata metadata, CountDownLatch latch) {
        try {
            return pipeline.run(metadata);
        } catch (Exception e) {
            LOGGER.error("Could not run single sample pipeline. ", e);
            latch.countDown();
            return empty();
        }
    }

    private static PipelineState empty() {
        return new PipelineState();
    }

    private PipelineState combine(final CountDownAndTrapStatus trapReference, final CountDownAndTrapStatus trapTumor,
            final SomaticRunMetadata metadata) {
        PipelineState combined = empty();
        metadata.maybeReference().ifPresent(reference -> {
            checkState(trapReference, "Reference");
            combined.combineWith(trapReference.trappedState);
        });
        metadata.maybeTumor().ifPresent(tumor -> {
            checkState(trapTumor, "Tumor");
            combined.combineWith(trapTumor.trappedState);
        });
        return combined;
    }

    private void checkState(final CountDownAndTrapStatus trap, final String type) {
        if (trap.trappedState == null && trap.trappedAlignmentOutput == null) {
            throw new IllegalStateException(String.format("%s sample pipeline returned a null state. Failing pipeline run.", type));
        }
    }

    private static void waitForSingleSamples(final CountDownLatch bothSingleSamplesComplete) {
        try {
            bothSingleSamplesComplete.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CountDownAndTrapStatus implements CompletionHandler {

        private final CountDownLatch latch;
        private final boolean trapAlignment;
        private PipelineState trappedState;
        private AlignmentOutput trappedAlignmentOutput;

        private CountDownAndTrapStatus(final CountDownLatch latch, final boolean trapAlignment) {
            this.latch = latch;
            this.trapAlignment = trapAlignment;
        }

        @Override
        public void handleAlignmentComplete(final AlignmentOutput output) {
            if (trapAlignment) {
                trappedAlignmentOutput = output;
                trappedState = new PipelineState();
                trappedState.add(output);
                latch.countDown();
            }
        }

        @Override
        public void handleSingleSampleComplete(final PipelineState state) {
            if (!trapAlignment) {
                trappedState = state;
                latch.countDown();
            }
        }
    }
}
