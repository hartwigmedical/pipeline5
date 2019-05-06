package com.hartwig.pipeline.tertiary.cobalt;

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
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CobaltTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Cobalt victim;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        final Storage storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        MockResource.addToStorage(storage, "cobalt-gc", "gc.cnp");
        victim = new Cobalt(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsCobaltFileGoogleStorageLocation() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        CobaltOutput output = victim.run(TestInputs.defaultPair());
        assertThat(output).isEqualTo(CobaltOutput.builder()
                .status(JobStatus.SUCCESS)
                .cobaltFile(GoogleStorageLocation.of(RUNTIME_BUCKET + "/cobalt", "results/tumor.cobalt"))
                .build());
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(victim.run(TestInputs.defaultPair()).status()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void runsCobaltOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains("java -Xmx8G -cp "
                + "/data/tools/cobalt/1.6/cobalt.jar com.hartwig.hmftools.cobalt.CountBamLinesApplication -reference reference "
                + "-reference_bam /data/input/reference.bam -tumor tumor -tumor_bam /data/input/tumor.bam -output_dir /data/output "
                + "-threads 16 -gc_profile /data/resources/gc.cnp");
    }

    @Test
    public void downloadsInputBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.bam /data/input/tumor.bam",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.bam /data/input/reference.bam",
                "gsutil -qm cp gs://run-tumor/aligner/results/tumor.bam.bai /data/input/tumor.bam.bai",
                "gsutil -qm cp gs://run-reference/aligner/results/reference.bam.bai /data/input/reference.bam.bai");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/* gs://run-reference-tumor/cobalt/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(JobStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }

}