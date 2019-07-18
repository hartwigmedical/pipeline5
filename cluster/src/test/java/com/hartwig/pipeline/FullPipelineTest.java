package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metrics.BamMetricsOutput;

import org.junit.Before;
import org.junit.Test;

public class FullPipelineTest {

    private SingleSamplePipeline reference;
    private SingleSamplePipeline tumor;
    private SomaticPipeline somatic;
    private FullPipeline victim;

    @Before
    public void setUp() throws Exception {
        reference = mock(SingleSamplePipeline.class);
        tumor = mock(SingleSamplePipeline.class);
        somatic = mock(SomaticPipeline.class);

        victim = new FullPipeline(reference, tumor, somatic, Executors.newSingleThreadExecutor());
    }

    @Test
    public void runsBothSingleSampleAndSomatic() throws Exception {
        when(reference.run()).thenReturn(succeeded());
        when(tumor.run()).thenReturn(succeeded());
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void failWhenReferencePipelineFails() throws Exception {
        when(reference.run()).thenReturn(failed());
        when(tumor.run()).thenReturn(succeeded());
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run();
    }

    @Test
    public void failWhenTumorPipelineFails() throws Exception {
        when(reference.run()).thenReturn(succeeded());
        when(tumor.run()).thenReturn(failed());
        when(somatic.run()).thenReturn(succeeded());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
        verify(somatic, never()).run();
    }

    @Test
    public void failWhenSomaticPipelineFails() throws Exception {
        when(reference.run()).thenReturn(succeeded());
        when(tumor.run()).thenReturn(succeeded());
        when(somatic.run()).thenReturn(failed());
        assertThat(victim.run().status()).isEqualTo(PipelineStatus.FAILED);
    }

    private static PipelineState succeeded() {
        return new PipelineState();
    }

    private static PipelineState failed() {
        PipelineState failedState = succeeded();
        failedState.add(BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample("test").build());
        return failedState;
    }

}