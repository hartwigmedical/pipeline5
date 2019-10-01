package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class OutputStorageTest {

    private OutputStorage<BamMetricsOutput, SingleSampleRunMetadata> victim;
    private RuntimeBucket runtimeBucket;

    @Before
    public void setUp() throws Exception {
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.name()).thenReturn(TestInputs.namespacedBucket(TestInputs.referenceSample(), BamMetrics.NAMESPACE));
        victim = new OutputStorage<>(ResultsDirectory.defaultDirectory(), Arguments.testDefaults(), metadata -> runtimeBucket);
    }

    @Test
    public void waitsForBamMetricsOutput() {
        Blob success = TestBlobs.blob(BashStartupScript.JOB_SUCCEEDED_FLAG);
        when(runtimeBucket.get(success.getName())).thenReturn(null).thenReturn(success);
        assertThat(victim.get(TestInputs.referenceRunMetadata(), new BamMetrics(TestInputs.referenceAlignmentOutput())).metricsOutputFile())
                .isEqualTo(TestInputs.referenceMetricsOutput().metricsOutputFile());
    }

    @Test
    public void returnsSkippedOutputWhenSkipped() {
        victim = new OutputStorage<>(ResultsDirectory.defaultDirectory(),
                Arguments.testDefaultsBuilder().runBamMetrics(false).build(),
                metadata -> runtimeBucket);
        assertThat(victim.get(TestInputs.referenceRunMetadata(), new BamMetrics(TestInputs.referenceAlignmentOutput())).status()).isEqualTo(
                PipelineStatus.SKIPPED);
    }
}