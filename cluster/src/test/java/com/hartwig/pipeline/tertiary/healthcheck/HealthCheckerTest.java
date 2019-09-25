package com.hartwig.pipeline.tertiary.healthcheck;

import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_HEALTHCHECKER_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.inFile;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultPair;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.BucketInputOutput;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HealthCheckerTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor-test";
    private static final Arguments ARGUMENTS = Arguments.testDefaults();

    private ComputeEngine computeEngine;
    private HealthChecker victim;
    private Storage storage;
    private Bucket bucket;
    private BucketInputOutput gs;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        returnHealthCheck(bucket, "tumor.HealthCheckSucceeded");
        gs = new BucketInputOutput(RUNTIME_BUCKET);
        victim = new HealthChecker(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedWhenTertiaryDisabledInArguments() {
        victim = new HealthChecker(Arguments.testDefaultsBuilder().runTertiary(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        HealthCheckOutput output = runVictim();
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsSkippedWhenShallow() {
        victim = new HealthChecker(Arguments.testDefaultsBuilder().shallow(true).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        HealthCheckOutput output = runVictim();
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsHealthCheckerOutputDirectory() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        HealthCheckOutput output = runVictim();
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/health_checker", "results"));
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(runVictim().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsHealthCheckApplicationOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "java -Xmx10G -jar " + TOOLS_HEALTHCHECKER_JAR + " -reference reference"
                        + " -tumor tumor -metrics_dir " + inFile("metrics")
                        + " -amber_dir " + inFile("amber")
                        + " -purple_dir " + inFile("purple")
                        + " -output_dir " + OUT_DIR);
    }

    @Test
    public void downloadsInputMetricsPurpleAndAmberOutput() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue()
                .startupCommand()
                .asUnixString()).contains(new BucketInputOutput("run-reference").input("reference.wgsmetrics",
                "metrics/reference.wgsmetrics"),
                new BucketInputOutput("run-tumor").input("tumor.wgsmetrics", "metrics/tumor.wgsmetrics"),
                gs.input("purple/*", "purple/"),
                gs.input("amber/*", "amber"));
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(gs.output("health_checker/results"));
    }

    @Test
    public void returnsStatusFailsWhenHealthCheckerReportsFailure() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        returnHealthCheck(bucket, "tumor.HealthCheckFailed");
        assertThat(runVictim().status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    private void returnHealthCheck(final Bucket bucket, final String status) {
        Blob blob = TestBlobs.blob(status);
        Page<Blob> page = pageOf(blob);
        when(bucket.list(Storage.BlobListOption.prefix(HealthChecker.NAMESPACE + "/results/tumor"))).thenReturn(page);
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }

    private HealthCheckOutput runVictim() {
        return victim.run(defaultSomaticRunMetadata(),
                defaultPair(),
                BamMetricsOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeMetricsOutputFile(GoogleStorageLocation.of("run-reference", "reference.wgsmetrics"))
                        .sample(defaultPair().reference().sample())
                        .build(),
                BamMetricsOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeMetricsOutputFile(GoogleStorageLocation.of("run-tumor", "tumor.wgsmetrics"))
                        .sample(defaultPair().tumor().sample())
                        .build(),
                AmberOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "amber", true))
                        .build(),
                PurpleOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "purple", true))
                        .build());
    }
}