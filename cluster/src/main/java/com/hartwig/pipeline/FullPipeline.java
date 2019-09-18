package com.hartwig.pipeline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import com.hartwig.pipeline.metadata.CompletionHandler;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

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
    private final SomaticRunMetadata metadata;

    FullPipeline(final SingleSamplePipeline referencePipeline, final SingleSamplePipeline tumorPipeline,
            final SomaticPipeline somaticPipeline, final ExecutorService executorService, final SingleSampleEventListener referenceApi,
            final SingleSampleEventListener tumorApi, final SomaticRunMetadata metadata) {
        this.referencePipeline = referencePipeline;
        this.tumorPipeline = tumorPipeline;
        this.somaticPipeline = somaticPipeline;
        this.executorService = executorService;
        this.referenceSampleEventListener = referenceApi;
        this.tumorSampleEventListener = tumorApi;
        this.metadata = metadata;
    }

    public PipelineState run() {

        final CountDownLatch bothSingleSamplesComplete = new CountDownLatch(2);

        CountDownAndTrapStatus trapReference = new CountDownAndTrapStatus(bothSingleSamplesComplete);
        CountDownAndTrapStatus trapTumor = new CountDownAndTrapStatus(bothSingleSamplesComplete);
        referenceSampleEventListener.register(trapReference);
        tumorSampleEventListener.register(trapTumor);
        executorService.submit(() -> runPipeline(referencePipeline, metadata.reference(), bothSingleSamplesComplete));
        executorService.submit(() -> runPipeline(tumorPipeline, metadata.tumor(), bothSingleSamplesComplete));
        waitForSingleSamples(bothSingleSamplesComplete);
        PipelineState singleSampleState = combine(trapReference, trapTumor);
        if (singleSampleState.shouldProceed()) {
            return singleSampleState.combineWith(somaticPipeline.run());
        } else {
            return singleSampleState;
        }
    }

    private void runPipeline(SingleSamplePipeline pipeline, SingleSampleRunMetadata metadata, CountDownLatch latch) {
        try {
            pipeline.run(metadata);
        } catch (Exception e) {
            LOGGER.error("Could not run single sample pipeline. ", e);
            latch.countDown();
        }
    }

    private PipelineState combine(final CountDownAndTrapStatus trapReference, final CountDownAndTrapStatus trapTumor) {
        checkState(trapReference, "Reference");
        checkState(trapTumor, "Tumor");
        return new PipelineState().combineWith(trapReference.trappedState).combineWith(trapTumor.trappedState);
    }

    private void checkState(final CountDownAndTrapStatus trap, final String type) {
        if (trap.trappedState == null) {
            throw new IllegalStateException(String.format("%s sample pipeline returned a null state. Failing pipeline run.", type));
        }
    }

    private static void waitForSingleSamples(final CountDownLatch bothSingleSamplesComplete) {
        try {
            bothSingleSamplesComplete.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new RuntimeException(e);
        }
    }

    private static class CountDownAndTrapStatus implements CompletionHandler {

        private final CountDownLatch latch;
        private PipelineState trappedState;

        private CountDownAndTrapStatus(final CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void handleAlignmentComplete(final PipelineState status) {
            trappedState = status;
            latch.countDown();
        }
    }
}
