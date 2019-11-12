package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class FullPipelineTest {

    private static final SomaticRunMetadata METADATA = TestInputs.defaultSomaticRunMetadata();
    private SingleSamplePipeline reference;
    private SingleSamplePipeline tumor;
    private SomaticPipeline somatic;
    private FullPipeline victim;
    private SingleSampleEventListener referenceListener;
    private SingleSampleEventListener tumorListener;

    @Before
    public void setUp() throws Exception {
        reference = mock(SingleSamplePipeline.class);
        tumor = mock(SingleSamplePipeline.class);
        somatic = mock(SomaticPipeline.class);

        referenceListener = new SingleSampleEventListener();
        tumorListener = new SingleSampleEventListener();
        victim = new FullPipeline(reference, tumor, somatic, Executors.newCachedThreadPool(), referenceListener, tumorListener, METADATA);
    }

    @Test
    public void runsBothSingleSampleAndSomatic() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, succeeded(), succeeded()));

        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void failWhenReferenceAlignmentFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, failed(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, succeeded(), succeeded()));
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run();
    }

    @Test
    public void failWhenTumorAlignmentFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, failed(), succeeded()));
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run();
    }

    @Test
    public void failWhenReferencePipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), failed()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, succeeded(), succeeded()));
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failWhenTumorPipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, succeeded(), failed()));
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failWhenSomaticPipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, succeeded(), succeeded()));
        when(somatic.run()).thenReturn(failed());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void supportsSingleSampleIni() throws Exception {
        victim = new FullPipeline(reference,
                tumor,
                somatic,
                Executors.newCachedThreadPool(),
                referenceListener,
                tumorListener,
                TestInputs.defaultSingleSampleRunMetadata());
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, succeeded(), succeeded()));
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    private static PipelineState succeeded() {
        return new PipelineState();
    }

    private static PipelineState failed() {
        PipelineState failedState = succeeded();
        failedState.add(BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample("test").build());
        return failedState;
    }

    private static Answer<PipelineState> callHandlers(final SingleSampleEventListener listener, final PipelineState alignmentState,
            final PipelineState pipelineState) {
        return invocation -> {
            listener.getHandlers().forEach(handler -> handler.handleAlignmentComplete(alignmentState));
            listener.getHandlers().forEach(handler -> handler.handleSingleSampleComplete(pipelineState));
            return pipelineState;
        };
    }
}