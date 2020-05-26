package com.hartwig.pipeline.tertiary.healthcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class HealthCheckerTest extends TertiaryStageTest<HealthCheckOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        returnHealthCheck(runtimeBucket, "tumor.HealthCheckSucceeded");
    }

    @Override
    protected Stage<HealthCheckOutput, SomaticRunMetadata> createVictim() {
        return new HealthChecker(TestInputs.referenceMetricsOutput(),
                TestInputs.tumorMetricsOutput(),
                TestInputs.amberOutput(),
                TestInputs.purpleOutput());
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of("mkdir -p /data/input/metrics",
                "mkdir -p /data/input/amber",
                "mkdir -p /data/input/purple",
                input("run-reference-test/bam_metrics/results/reference.wgsmetrics", "metrics/reference.wgsmetrics"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "metrics/tumor.wgsmetrics"),
                input(expectedRuntimeBucketName() + "/amber/results/", "amber"),
                input(expectedRuntimeBucketName() + "/purple/results/", "purple"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx10G -jar $TOOLS_DIR/health-checker/3.1/health-checker.jar -reference reference -tumor "
                + "tumor -metrics_dir /data/input/metrics -amber_dir /data/input/amber -purple_dir /data/input/purple -output_dir "
                + "/data/output");
    }

    @Test
    public void returnsStatusQcFailsWhenHealthCheckerReportsFailure() {
        returnHealthCheck(runtimeBucket, "tumor.HealthCheckFailed");
        assertThat(victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory()).status()).isEqualTo(
                PipelineStatus.QC_FAILED);
    }

    @Test
    public void returnsStatusFailsWhenHealthCheckerReportsFailureNothingFailed() {
        whenBucketChecked(runtimeBucket).thenReturn(Collections.emptyList());
        assertThat(victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory()).status()).isEqualTo(
                PipelineStatus.FAILED);
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final HealthCheckOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo(expectedRuntimeBucketName() + "/" + HealthChecker.NAMESPACE);
        assertThat(output.outputDirectory().path()).isEqualTo("results");
    }

    private void returnHealthCheck(final RuntimeBucket bucket, final String status) {
        Blob blob = TestBlobs.blob(status);
        whenBucketChecked(bucket).thenReturn(Collections.singletonList(blob));
    }

    private OngoingStubbing<List<Blob>> whenBucketChecked(final RuntimeBucket bucket) {
        return when(bucket.list("results/tumor"));
    }
}