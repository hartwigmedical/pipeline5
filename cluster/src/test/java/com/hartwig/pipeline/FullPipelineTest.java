package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.cram.cleanup.Cleanup;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.OutputPublisher;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

public class FullPipelineTest {

    private static final SomaticRunMetadata METADATA = TestInputs.defaultSomaticRunMetadata();
    private SingleSamplePipeline reference;
    private SingleSamplePipeline tumor;
    private SomaticPipeline somatic;
    private FullPipeline victim;
    private SingleSampleEventListener referenceListener;
    private SingleSampleEventListener tumorListener;
    private Cleanup cleanup;
    private OutputPublisher outputPublisher;

    @Before
    public void setUp() throws Exception {
        reference = mock(SingleSamplePipeline.class);
        tumor = mock(SingleSamplePipeline.class);
        somatic = mock(SomaticPipeline.class);
        cleanup = mock(Cleanup.class);
        outputPublisher = mock(OutputPublisher.class);

        referenceListener = new SingleSampleEventListener();
        tumorListener = new SingleSampleEventListener();
        victim = new FullPipeline(reference,
                tumor,
                somatic,
                METADATA,
                Executors.newCachedThreadPool(),
                referenceListener,
                tumorListener,
                cleanup,
                outputPublisher);
    }

    @Test
    public void runsBothSingleSampleAndSomatic() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));

        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void failWhenReferenceAlignmentFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener,
                failedAlignment(TestInputs.referenceAlignmentOutput()),
                succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run(TestInputs.defaultPair());
    }

    @Test
    public void failWhenTumorAlignmentFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener,
                failedAlignment(TestInputs.tumorAlignmentOutput()),
                succeeded()));
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run(TestInputs.defaultPair());
    }

    @Test
    public void failWhenReferencePipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), failed()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failWhenTumorPipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), failed()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failWhenSomaticPipelineFails() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(failed());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void notifiesSetMetadataApiOnSuccessfulSomaticRun() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());

        victim.run();

        ArgumentCaptor<PipelineState> stateCaptor = ArgumentCaptor.forClass(PipelineState.class);
        verify(outputPublisher, times(1)).publish(stateCaptor.capture(), eq(METADATA));
        assertThat(stateCaptor.getValue().status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void notifiesSetMetadataApiOnFailedSomaticRun() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(failed());

        victim.run();

        ArgumentCaptor<PipelineState> stateCaptor = ArgumentCaptor.forClass(PipelineState.class);
        verify(outputPublisher, times(1)).publish(stateCaptor.capture(), eq(METADATA));
        assertThat(stateCaptor.getValue().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsCleanupOnSuccessfulSomaticRun() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());

        victim.run();

        verify(cleanup, times(1)).run(METADATA);
    }

    @Test
    public void doesNotRunCleanupOnFailedSomaticRun() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(failed());

        victim.run();

        verify(cleanup, never()).run(METADATA);
    }

    @Test
    public void doesNotRunCleanupOnFailedTransfer() throws Exception {
        when(reference.run(METADATA.reference())).then(callHandlers(referenceListener, TestInputs.referenceAlignmentOutput(), succeeded()));
        when(tumor.run(METADATA.tumor())).then(callHandlers(tumorListener, TestInputs.tumorAlignmentOutput(), succeeded()));
        when(somatic.run(TestInputs.defaultPair())).thenReturn(succeeded());
        doThrow(new NullPointerException()).when(outputPublisher).publish(any(PipelineState.class), any(SomaticRunMetadata.class));

        try {
            victim.run();
        } catch (Exception e) {
            // continue
        }
        verify(cleanup, never()).run(any());
    }

    private static PipelineState succeeded() {
        return new PipelineState();
    }

    private static PipelineState failed() {
        PipelineState failedState = succeeded();
        failedState.add(BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample("test").build());
        return failedState;
    }

    private static ImmutableAlignmentOutput failedAlignment(final AlignmentOutput from) {
        return AlignmentOutput.builder().from(from).status(PipelineStatus.FAILED).build();
    }

    private static Answer<PipelineState> callHandlers(final SingleSampleEventListener listener, final AlignmentOutput alignmentState,
            final PipelineState pipelineState) {
        return invocation -> {
            listener.getHandlers().forEach(handler -> handler.handleAlignmentComplete(alignmentState));
            listener.getHandlers().forEach(handler -> handler.handleSingleSampleComplete(pipelineState));
            return pipelineState;
        };
    }
}