package com.hartwig.pipeline.flagstat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class FlagstatTest extends StageTest<FlagstatOutput, SingleSampleRunMetadata> {

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaults();
    }

    @Override
    public void disabledAppropriately() {
        // cannot be disabled
    }

    @Override
    protected Stage<FlagstatOutput, SingleSampleRunMetadata> createVictim() {
        return new Flagstat(TestInputs.referenceAlignmentOutput());
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input("run-reference-test/aligner/results/reference.bam", "reference.bam"));
    }

    @Override
    protected List<String> expectedResources() {
        return Collections.emptyList();
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "(/opt/tools/sambamba/0.6.8/sambamba flagstat -t $(grep -c '^processor' /proc/cpuinfo) /data/input/reference.bam > "
                        + "/data/output/reference.flagstat)");
    }

    @Override
    protected boolean validateOutput(final FlagstatOutput output) {
        return output.status() == PipelineStatus.SUCCESS && output.reportComponents().size() == 3 && output.name()
                .equals(victim.namespace());
    }

    /* private static final String RUNTIME_BUCKET = "run-reference-test";
    private ComputeEngine computeEngine;
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private Flagstat victim;

    @Before
    public void setUp() throws Exception {
        computeEngine = mock(ComputeEngine.class);
        final Storage storage = mock(Storage.class);
        final Bucket bucket = mock(Bucket.class);
        when(bucket.getName()).thenReturn(RUNTIME_BUCKET);
        when(storage.get(RUNTIME_BUCKET)).thenReturn(bucket);
        victim = new Flagstat(ARGUMENTS, computeEngine, storage, ResultsDirectory.defaultDirectory());
    }

    @Test
    public void returnsStatusFailedWhenJobFailsOnComputeEngine() {
        when(computeEngine.submit(any(), any())).thenReturn(PipelineStatus.FAILED);
        assertThat(victim.run(referenceRunMetadata(), referenceAlignmentOutput()).status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void runsSamtoolsFlagstatOnComputeEngine() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "(/opt/tools/sambamba/0.6.8/sambamba flagstat -t $(grep -c '^processor' /proc/cpuinfo) /data/input/reference.bam > "
                        + "/data/output/reference.flagstat)");
    }

    @Test
    public void downloadsInputBam() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm cp -n gs://run-reference/aligner/results/reference.bam /data/input/reference.bam");
    }

    @Test
    public void uploadsOutputDirectory() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor = captureAndReturnSuccess();
        victim.run(referenceRunMetadata(), referenceAlignmentOutput());
        assertThat(jobDefinitionArgumentCaptor.getValue().startupCommand().asUnixString()).contains(
                "gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r /data/output/ gs://run-reference-test/flagstat/results");
    }

    private ArgumentCaptor<VirtualMachineJobDefinition> captureAndReturnSuccess() {
        ArgumentCaptor<VirtualMachineJobDefinition> jobDefinitionArgumentCaptor =
                ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(computeEngine.submit(any(), jobDefinitionArgumentCaptor.capture())).thenReturn(PipelineStatus.SUCCESS);
        return jobDefinitionArgumentCaptor;
    }*/

}