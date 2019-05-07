package com.hartwig.pipeline.tertiary.healthcheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HealthCheckerTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private HealthChecker victim;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        final Storage storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        victim = new HealthChecker(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsHealthCheckerOutputFile() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        HealthCheckOutput output = runVictim();
        assertThat(output).isEqualTo(HealthCheckOutput.builder()
                .status(JobStatus.SUCCESS)
                .outputFile(GoogleStorageLocation.of(RUNTIME_BUCKET + "/health_checker", "results/HealthCheck.out"))
                .build());
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(runVictim().status()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void runsHealthCheckApplicationOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "java -Xmx10G -jar /data/tools/health-checker/2.4/health-checker -run_dir /data/input -report_file_path /data/output");
    }

    @Test
    public void runsPerlHealthCheckEvaluationOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "perl /data/tools/health-checker/2.4/do_healthcheck_qctests.pl  --healthcheck-log-file /data/output/HealthCheck.out");
    }

    @Test
    public void downloadsInputMetricsSomaticVcfPurpleAndAmberOutput() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-reference/reference.wgsmetrics /data/input/reference.wgsmetrics",
                "gsutil -qm cp gs://run-tumor/tumor.wgsmetrics /data/input/tumor.wgsmetrics",
                "gsutil -qm cp gs://run-reference-tumor/somatic.vcf /data/input/somatic.vcf",
                "gsutil -qm cp gs://run-reference-tumor/purple/* /data/input/",
                "gsutil -qm cp gs://run-reference-tumor/amber/* /data/input/");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/* gs://run-reference-tumor/health_checker/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(JobStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }

    private HealthCheckOutput runVictim() {
        return victim.run(TestInputs.defaultPair(),
                GoogleStorageLocation.of("run-reference", "reference.wgsmetrics"),
                GoogleStorageLocation.of("run-tumor", "tumor.wgsmetrics"),
                GoogleStorageLocation.of(RUNTIME_BUCKET, "somatic.vcf"),
                GoogleStorageLocation.of(RUNTIME_BUCKET, "purple", true),
                GoogleStorageLocation.of(RUNTIME_BUCKET, "amber", true));
    }
}