package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultPair;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BatchInputDownload;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.testsupport.MockResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StructuralCallerTest {
    private static final String RUNTIME_JOINT_BUCKET = "run-reference-tumor-test";

    private ComputeEngine computeEngine;
    private StructuralCaller victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_JOINT_BUCKET);
        when(storage.get(RUNTIME_JOINT_BUCKET)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        MockResource.addToStorage(storage, REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, GRIDSS_CONFIG, "config.properties", "blacklist.bed");
        MockResource.addToStorage(storage, GRIDSS_PON, "gridss.pon");
        computeEngine = mock(ComputeEngine.class);
        victim = new StructuralCaller(Arguments.testDefaults(), computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedIfDisabledInArguments() {
        assertThat(new StructuralCaller(Arguments.testDefaultsBuilder().runStructuralCaller(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory()).run(defaultSomaticRunMetadata(), defaultPair())
                .status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void shouldDownloadResources() {
        String bashBeforeJava = getBashBeforeJava();
        assertThat(bashBeforeJava).contains(resourceDownloadBash(RUNTIME_JOINT_BUCKET, REFERENCE_GENOME + "/*"));
        assertThat(bashBeforeJava).contains(resourceDownloadBash(RUNTIME_JOINT_BUCKET, GRIDSS_CONFIG + "/*"));
        assertThat(bashBeforeJava).contains(resourceDownloadBash(RUNTIME_JOINT_BUCKET, GRIDSS_PON + "/*"));
    }

    @Test
    public void shouldSetUlimitBeforeAnyJavaCommandsAreCalled() {
        assertThat(getBashBeforeJava()).contains("\nulimit -n 102400 ");
    }

    @Test
    public void shouldExportPathWithBwaOnItBeforeAnyJavaCommandIsCalled() {
        assertThat(getBashBeforeJava()).contains("\nexport PATH=\"${PATH}:/opt/tools/bwa/0.7.17\" ");
    }

    @Test
    public void shouldBatchDownloadInputBamsAndBais() {
        InputDownload referenceBam = new InputDownload(defaultPair().reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(defaultPair().reference().finalBaiLocation());
        InputDownload tumorBam = new InputDownload(defaultPair().tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(defaultPair().tumor().finalBaiLocation());
        BatchInputDownload batchCommand = new BatchInputDownload(referenceBam, referenceBai, tumorBam, tumorBai);
        assertThat(getBashBeforeJava()).contains(batchCommand.asBash());
    }

    @Test
    public void returnsFailedStatusWhenJobFails() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(defaultSomaticRunMetadata(), defaultPair()).status()).isEqualTo(PipelineStatus.FAILED);
    }

    private String getBashBeforeJava() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinition = ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinition.capture())).thenReturn(PipelineStatus.SUCCESS);
        victim.run(defaultSomaticRunMetadata(), defaultPair());
        String all = jobDefinition.getValue().startupCommand().asUnixString();
        return all.substring(0, all.indexOf("java"));
    }

    private String resourceDownloadBash(String bucketName, String path) {
        return format("\ngsutil -qm cp gs://%s/structural_caller/%s /data/resources ", bucketName, path);
    }
}