package com.hartwig.pipeline.tertiary.healthcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.input.SomaticRunMetadata;
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
                TestInputs.referenceFlagstatOutput(),
                TestInputs.tumorFlagstatOutput(),
                TestInputs.purpleOutput());
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of("mkdir -p /data/input/metrics",
                "mkdir -p /data/input/flagstat",
                "mkdir -p /data/input/purple",
                input("run-reference-test/bam_metrics/results/reference.wgsmetrics", "metrics/reference.wgsmetrics"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "metrics/tumor.wgsmetrics"),
                input("run-reference-test/flagstat/reference.flagstat", "flagstat/reference.flagstat"),
                input("run-tumor-test/flagstat/tumor.flagstat", "flagstat/tumor.flagstat"),
                input(expectedRuntimeBucketName() + "/purple/results/", "purple"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx10G -jar /opt/tools/health-checker/3.4/health-checker.jar -purple_dir /data/input/purple -output_dir /data/output "
                        + "-tumor tumor -tum_wgs_metrics_file /data/input/metrics/tumor.wgsmetrics -tum_flagstat_file "
                        + "/data/input/flagstat/tumor.flagstat -reference reference "
                        + "-ref_wgs_metrics_file /data/input/metrics/reference.wgsmetrics -ref_flagstat_file "
                        + "/data/input/flagstat/reference.flagstat");
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
    protected void validatePersistedOutput(final HealthCheckOutput output) {
        // no validation
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final HealthCheckOutput output) {
        // no validation
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