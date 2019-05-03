package com.hartwig.pipeline.tertiary.amber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.NamespacedResults;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AmberTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Amber victim;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        final Storage storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        MockResource.addToStorage(storage, "reference_genome", "reference.fasta");
        MockResource.addToStorage(storage, "amber-pon", "amber.bed");
        victim = new Amber(ARGUMENTS, computeEngine, storage, NamespacedResults.of(Amber.RESULTS_NAMESPACE));
    }

    @Test
    public void returnsAmberBafFileGoogleStorageLocation() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.SUCCESS);
        AmberOutput output = victim.run(TestInputs.defaultPair());
        assertThat(output).isEqualTo(AmberOutput.builder()
                .status(JobStatus.SUCCESS)
                .baf(GoogleStorageLocation.of(RUNTIME_BUCKET, "results/amber/tumor.amber.baf"))
                .build());
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(JobStatus.FAILED);
        assertThat(victim.run(TestInputs.defaultPair()).status()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void runsAmberOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains("java -Xmx32G -cp "
                + "/data/tools/amber/2.3/amber.jar com.hartwig.hmftools.amber.AmberApplication -reference reference -reference_bam "
                + "/data/input/reference.bam -tumor tumor -tumor_bam /data/input/tumor.bam -output_dir /data/output -threads 16 -ref_genome "
                + "/data/resources/reference.fasta -bed /data/resources/amber.bed");
    }

    @Test
    public void downloadsInputBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-tumor/tumor.bam /data/input/tumor.bam",
                "gsutil -qm cp gs://run-reference/reference.bam /data/input/reference.bam",
                "gsutil -qm cp gs://run-tumor/tumor.bam.bai /data/input/tumor.bam.bai",
                "gsutil -qm cp gs://run-reference/reference.bam.bai /data/input/reference.bam.bai");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(TestInputs.defaultPair());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/* gs://run-reference-tumor/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(JobStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }
}