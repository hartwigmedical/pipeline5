package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.amberOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.chordOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.cobaltOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.cuppaOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.healthCheckerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.orangeOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.paveGermlineOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.paveSomaticOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.peachOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.protectOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceFlagstatOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.sageGermlineOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.sageSomaticOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.sigsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.structuralCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.structuralCallerPostProcessOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorFlagstatOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.virusOutput;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.sage.SageSomaticCaller;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class SomaticPipelineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SomaticPipeline victim;
    private StructuralCaller structuralCaller;
    private SomaticMetadataApi setMetadataApi;
    private StageRunner<SomaticRunMetadata> stageRunner;
    private BlockingQueue<BamMetricsOutput> referenceMetricsOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<BamMetricsOutput> tumorMetricsOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue = new ArrayBlockingQueue<>(1);

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        structuralCaller = mock(StructuralCaller.class);
        setMetadataApi = mock(SomaticMetadataApi.class);
        when(setMetadataApi.get()).thenReturn(defaultSomaticRunMetadata());
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.outputBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        stageRunner = mock(StageRunner.class);
        victim = new SomaticPipeline(ARGUMENTS,
                stageRunner,
                referenceMetricsOutputQueue,
                tumorMetricsOutputQueue,
                referenceFlagstatOutputQueue,
                tumorFlagstatOutputQueue,
                setMetadataApi,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                new NoopPersistedDataset());
    }

    @Test
    public void runsAllSomaticStagesWhenAlignmentAndMetricsExist() {
        successfulRun();
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageSomaticOutput(),
                sageGermlineOutput(),
                structuralCallerOutput(),
                paveSomaticOutput(),
                paveGermlineOutput(),
                structuralCallerPostProcessOutput(),
                purpleOutput(),
                healthCheckerOutput(),
                linxOutput(),
                sigsOutput(),
                virusOutput(),
                chordOutput(),
                protectOutput(),
                peachOutput(),
                cuppaOutput(),
                orangeOutput());
    }

    @Test
    public void doesNotRunPurpleIfSomaticCallerFails() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        SageOutput failSomatic = SageOutput.builder(SageSomaticCaller.NAMESPACE).status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(failSomatic)
                .thenReturn(sageGermlineOutput())
                .thenReturn(structuralCallerOutput());
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                failSomatic,
                sageGermlineOutput(),
                structuralCallerOutput());
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunPurpleIfSvCallerFails() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        StructuralCallerOutput failSvOutput = StructuralCallerOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageSomaticOutput())
                .thenReturn(sageGermlineOutput())
                .thenReturn(failSvOutput);
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(amberOutput(),
                cobaltOutput(),
                failSvOutput,
                sageSomaticOutput(),
                sageGermlineOutput());
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunPurpleWhenGripssFails() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        StructuralCallerPostProcessOutput failGripss = StructuralCallerPostProcessOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageSomaticOutput())
                .thenReturn(sageGermlineOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(paveSomaticOutput())
                .thenReturn(paveGermlineOutput())
                .thenReturn(failGripss);
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageSomaticOutput(),
                sageGermlineOutput(),
                paveSomaticOutput(),
                paveGermlineOutput(),
                structuralCallerOutput(),
                failGripss);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunHealthCheckWhenPurpleFails() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        PurpleOutput failPurple = PurpleOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageSomaticOutput())
                .thenReturn(sageGermlineOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(paveSomaticOutput())
                .thenReturn(paveGermlineOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(failPurple);
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageSomaticOutput(),
                sageGermlineOutput(),
                paveSomaticOutput(),
                paveGermlineOutput(),
                structuralCallerOutput(),
                structuralCallerPostProcessOutput(),
                failPurple);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failsRunOnQcFailure() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageSomaticOutput())
                .thenReturn(sageGermlineOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(paveSomaticOutput())
                .thenReturn(paveGermlineOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(purpleOutput())
                .thenReturn(virusOutput())
                .thenReturn(HealthCheckOutput.builder().from(healthCheckerOutput()).status(PipelineStatus.QC_FAILED).build())
                .thenReturn(linxOutput())
                .thenReturn(sigsOutput())
                .thenReturn(chordOutput())
                .thenReturn(cuppaOutput())
                .thenReturn(peachOutput())
                .thenReturn(protectOutput())
                .thenReturn(orangeOutput());
        PipelineState state = victim.run(TestInputs.defaultPair());
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    @Test
    public void skipsStructuralCallerIfSingleSampleRun() {
        when(setMetadataApi.get()).thenReturn(SomaticRunMetadata.builder()
                .from(defaultSomaticRunMetadata())
                .maybeTumor(Optional.empty())
                .build());
        victim.run(TestInputs.defaultPair());
        verifyZeroInteractions(stageRunner, structuralCaller);
    }

    private void successfulRun() {
        bothMetricsAvailable();
        bothFlagstatsAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageSomaticOutput())
                .thenReturn(sageGermlineOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(paveSomaticOutput())
                .thenReturn(paveGermlineOutput())
                .thenReturn(structuralCallerPostProcessOutput())
                .thenReturn(purpleOutput())
                .thenReturn(virusOutput())
                .thenReturn(healthCheckerOutput())
                .thenReturn(linxOutput())
                .thenReturn(sigsOutput())
                .thenReturn(chordOutput())
                .thenReturn(cuppaOutput())
                .thenReturn(peachOutput())
                .thenReturn(protectOutput())
                .thenReturn(orangeOutput());
    }

    private void bothMetricsAvailable() {
        try {
            tumorMetricsOutputQueue.put(tumorMetricsOutput());
            referenceMetricsOutputQueue.put(referenceMetricsOutput());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void bothFlagstatsAvailable() {
        try {
            tumorFlagstatOutputQueue.put(referenceFlagstatOutput());
            referenceFlagstatOutputQueue.put(tumorFlagstatOutput());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}