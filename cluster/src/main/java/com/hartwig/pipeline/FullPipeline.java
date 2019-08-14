package com.hartwig.pipeline;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import com.hartwig.pipeline.metadata.CompletionHandler;
import com.hartwig.pipeline.metadata.LocalSampleMetadataApi;

public class FullPipeline {

    private final SingleSamplePipeline referencePipeline;
    private final SingleSamplePipeline tumorPipeline;
    private final SomaticPipeline somaticPipeline;
    private final ExecutorService executorService;
    private final LocalSampleMetadataApi referenceApi;
    private final LocalSampleMetadataApi tumorApi;

    FullPipeline(final SingleSamplePipeline referencePipeline, final SingleSamplePipeline tumorPipeline,
            final SomaticPipeline somaticPipeline, final ExecutorService executorService, final LocalSampleMetadataApi referenceApi,
            final LocalSampleMetadataApi tumorApi) {
        this.referencePipeline = referencePipeline;
        this.tumorPipeline = tumorPipeline;
        this.somaticPipeline = somaticPipeline;
        this.executorService = executorService;
        this.referenceApi = referenceApi;
        this.tumorApi = tumorApi;
    }

    public PipelineState run() {

        final CountDownLatch bothSingleSamplesComplete = new CountDownLatch(2);

        CountDownAndTrapStatus trapReference = new CountDownAndTrapStatus(bothSingleSamplesComplete);
        CountDownAndTrapStatus trapTumor = new CountDownAndTrapStatus(bothSingleSamplesComplete);
        referenceApi.register(trapReference);
        tumorApi.register(trapTumor);
        executorService.submit(referencePipeline::run);
        executorService.submit(tumorPipeline::run);
        waitForSingleSamples(bothSingleSamplesComplete);
        PipelineState singleSampleState = combine(trapReference, trapTumor);
        if (singleSampleState.shouldProceed()) {
            return singleSampleState.combineWith(somaticPipeline.run());
        } else {
            return singleSampleState;
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
