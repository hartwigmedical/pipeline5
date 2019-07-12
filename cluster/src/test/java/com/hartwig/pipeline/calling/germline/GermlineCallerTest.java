package com.hartwig.pipeline.calling.germline;

import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;

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
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.testsupport.MockResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GermlineCallerTest {

    private static final String RUNTIME_BUCKET = "run-reference";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private GermlineCaller victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(any())).thenReturn(copyWriter);
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.DBNSFP, "dbsnfp.txt.gz");
        MockResource.addToStorage(storage, ResourceNames.GONL, "gonl.vcf.gz");
        MockResource.addToStorage(storage, ResourceNames.COSMIC, "cosmic_collapsed.vcf.gz");
        MockResource.addToStorage(storage, ResourceNames.SNPEFF, "snpeff.config", "database.zip");
        MockResource.addToStorage(storage, ResourceNames.DBSNPS, "dbsnps.vcf");
        victim = new GermlineCaller(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void shouldCopySnpeffDatabaseToResourcesDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp gs://run-reference/germline_caller/snpeff/* /data/resources");
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "unzip -d /data/resources /data/resources/database.zip");
    }

    @Test
    public void returnsSkippedWhenDisabledInArguments() {
        victim = new GermlineCaller(Arguments.testDefaultsBuilder().runGermlineCaller(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        GermlineCallerOutput output = victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(referenceRunMetadata(), referenceAlignmentOutput()).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void downloadsInputBamsAndBais() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -n gs://run-reference/aligner/results/reference.bam /data/input/reference.bam",
                "gsutil -qm cp -n gs://run-reference/aligner/results/reference.bam.bai /data/input/reference.bam.bai");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/ gs://run-reference/germline_caller/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }
}