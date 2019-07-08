package com.hartwig.pipeline.tertiary.purple;

import static com.hartwig.pipeline.testsupport.TestInputs.defaultPair;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.tools.Versions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PurpleTest {

    private static final String RUNTIME_BUCKET = "run-reference-tumor";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Purple victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.GC_PROFILE, "gc_profile.cnp");
        victim = new Purple(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsSkippedWhenTertiaryDisabledInArguments() {
        victim = new Purple(Arguments.testDefaultsBuilder().runTertiary(false).build(),
                computeEngine,
                storage,
                ResultsDirectory.defaultDirectory());
        PurpleOutput output = runVictim();
        assertThat(output.status()).isEqualTo(PipelineStatus.SKIPPED);
    }

    @Test
    public void returnsPurpleOutputDirectory() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.SUCCESS);
        PurpleOutput output = runVictim();
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(RUNTIME_BUCKET + "/purple", "results", true));
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(runVictim().status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsPurpleOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "java -Xmx8G -jar " + "/data/tools/purple/" + Versions.PURPLE
                        + "/purple.jar -reference reference -tumor tumor -output_dir /data/output -amber "
                        + "/data/input -cobalt /data/input -gc_profile /data/resources/gc_profile.cnp -somatic_vcf /data/input/somatic.vcf "
                        + "-structural_vcf /data/input/structural.vcf -sv_recovery_vcf /data/input/sv_recovery.vcf -circos "
                        + "/data/tools/circos/0.69.6/bin/circos -threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Test
    public void downloadsInputVcfsCobaltAndAmberOutput() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -n gs://run-reference-tumor/somatic.vcf /data/input/somatic.vcf",
                "gsutil -qm cp -n gs://run-reference-tumor/structural.vcf /data/input/structural.vcf",
                "gsutil -qm cp -n gs://run-reference-tumor/sv_recovery.vcf /data/input/sv_recovery.vcf",
                "gsutil -qm cp -n gs://run-reference-tumor/amber/* /data/input/",
                "gsutil -qm cp -n gs://run-reference-tumor/cobalt/* /data/input/");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        runVictim();
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -r /data/output/ gs://run-reference-tumor/purple/results");
    }

    private PurpleOutput runVictim() {
        return victim.run(defaultSomaticRunMetadata(),
                defaultPair(),
                SomaticCallerOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeFinalSomaticVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "somatic.vcf"))
                        .build(),
                StructuralCallerOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeFilteredVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "structural.vcf"))
                        .maybeFilteredVcfIndex(GoogleStorageLocation.of(RUNTIME_BUCKET, "structural.vcf.tbi"))
                        .maybeFullVcf(GoogleStorageLocation.of(RUNTIME_BUCKET, "sv_recovery.vcf"))
                        .maybeFullVcfIndex(GoogleStorageLocation.of(RUNTIME_BUCKET, "sv_recovery.vcf.tbi"))
                        .build(),
                CobaltOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "cobalt", true))
                        .build(),
                AmberOutput.builder()
                        .status(PipelineStatus.SUCCESS)
                        .maybeOutputDirectory(GoogleStorageLocation.of(RUNTIME_BUCKET, "amber", true))
                        .build());
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }
}