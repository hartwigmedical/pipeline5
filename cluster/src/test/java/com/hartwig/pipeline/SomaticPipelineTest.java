package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.amberOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.bachelorOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.cobaltOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultPair;
import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.germlineCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.healthCheckerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.somaticCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.structuralCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.junit.Before;
import org.junit.Test;

public class SomaticPipelineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private AlignmentOutputStorage alignmentOutputStorage;
    private OutputStorage<BamMetricsOutput, SingleSampleRunMetadata> bamMetricsOutputStorage;
    private SomaticPipeline victim;
    private StructuralCaller structuralCaller;
    private SomaticMetadataApi setMetadataApi;
    private Cleanup cleanup;
    private StageRunner<SomaticRunMetadata> stageRunner;
    private OutputStorage<GermlineCallerOutput, SingleSampleRunMetadata> germlineCallerOutputStorage;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        alignmentOutputStorage = mock(AlignmentOutputStorage.class);
        bamMetricsOutputStorage = mock(OutputStorage.class);
        structuralCaller = mock(StructuralCaller.class);
        setMetadataApi = mock(SomaticMetadataApi.class);
        when(setMetadataApi.get()).thenReturn(defaultSomaticRunMetadata());
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        final FullSomaticResults fullSomaticResults = mock(FullSomaticResults.class);
        cleanup = mock(Cleanup.class);
        stageRunner = mock(StageRunner.class);
        germlineCallerOutputStorage = mock(OutputStorage.class);
        victim = new SomaticPipeline(ARGUMENTS,
                stageRunner,
                alignmentOutputStorage,
                bamMetricsOutputStorage,
                germlineCallerOutputStorage,
                setMetadataApi,
                pipelineResults,
                fullSomaticResults,
                cleanup,
                structuralCaller,
                Executors.newSingleThreadExecutor());
    }

    @Test(expected = IllegalStateException.class)
    public void failsIfTumorAlignmentNotAvailable() {
        victim.run();
    }

    @Test(expected = IllegalStateException.class)
    public void failsIfReferenceAlignmentNotAvailable() {
        when(alignmentOutputStorage.get(tumorRunMetadata())).thenReturn(Optional.of(tumorAlignmentOutput()));
        victim.run();
    }

    @Test
    public void runsAllSomaticStagesWhenAlignmentAndMetricsExist() {
        successfulRun();
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                somaticCallerOutput(),
                structuralCallerOutput(),
                purpleOutput(),
                healthCheckerOutput(),
                linxOutput(),
                bachelorOutput());
    }

    @Test
    public void doesNotRunPurpleIfAnyCallersFail() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(structuralCaller.run(defaultSomaticRunMetadata(), defaultPair())).thenReturn(structuralCallerOutput());
        SomaticCallerOutput failSomatic = SomaticCallerOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(failSomatic);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(), amberOutput(), failSomatic, structuralCallerOutput());
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunHealthCheckWhenPurpleFails() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(structuralCaller.run(defaultSomaticRunMetadata(), defaultPair())).thenReturn(structuralCallerOutput());
        PurpleOutput failPurple = PurpleOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(somaticCallerOutput())
                .thenReturn(failPurple);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                somaticCallerOutput(),
                structuralCallerOutput(),
                failPurple);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void notifiesSetMetadataApiOnSuccessfulRun() {
        successfulRun();
        victim.run();
        verify(setMetadataApi, times(1)).complete(PipelineStatus.SUCCESS, defaultSomaticRunMetadata());
    }

    @Test
    public void notifiesSetMetadataApiOnFailedRun() {
        failedRun();
        victim.run();
        verify(setMetadataApi, times(1)).complete(PipelineStatus.FAILED, defaultSomaticRunMetadata());
    }

    @Test
    public void runsCleanupOnSuccessfulRun() {
        successfulRun();
        victim.run();
        verify(cleanup, times(1)).run(defaultSomaticRunMetadata());
    }

    @Test
    public void doesNotRunCleanupOnFailedRun() {
        failedRun();
        victim.run();
        verify(cleanup, never()).run(defaultSomaticRunMetadata());
    }

    @Test
    public void doesNotRunCleanupOnFailedTransfer() {
        successfulRun();
        doThrow(new NullPointerException()).when(setMetadataApi).complete(PipelineStatus.SUCCESS, defaultSomaticRunMetadata());
        try {
            victim.run();
        } catch (Exception e) {
            // continue
        }
        verify(cleanup, never()).run(defaultSomaticRunMetadata());
    }

    @Test
    public void onlyDoesTransferAndCleanupIfSingleSampleRun() {
        when(alignmentOutputStorage.get(referenceRunMetadata())).thenReturn(Optional.of(referenceAlignmentOutput()));
        when(setMetadataApi.get()).thenReturn(SomaticRunMetadata.builder()
                .from(defaultSomaticRunMetadata())
                .maybeTumor(Optional.empty())
                .build());
        victim.run();
        verifyZeroInteractions(stageRunner, structuralCaller);
    }

    @Test
    public void failsRunOnQcFailure() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(structuralCaller.run(defaultSomaticRunMetadata(), defaultPair())).thenReturn(structuralCallerOutput());
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(somaticCallerOutput())
                .thenReturn(purpleOutput())
                .thenReturn(HealthCheckOutput.builder().from(healthCheckerOutput()).status(PipelineStatus.QC_FAILED).build())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput());
        PipelineState state = victim.run();
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    private void successfulRun() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(structuralCaller.run(defaultSomaticRunMetadata(), defaultPair())).thenReturn(structuralCallerOutput());
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(somaticCallerOutput())
                .thenReturn(purpleOutput())
                .thenReturn(healthCheckerOutput())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput());
    }

    private void failedRun() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(structuralCaller.run(defaultSomaticRunMetadata(), defaultPair())).thenReturn(structuralCallerOutput());
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(somaticCallerOutput())
                .thenReturn(PurpleOutput.builder().status(PipelineStatus.FAILED).build())
                .thenReturn(healthCheckerOutput());
    }

    private void bothAlignmentsAvailable() {
        when(alignmentOutputStorage.get(tumorRunMetadata())).thenReturn(Optional.of(tumorAlignmentOutput()));
        when(alignmentOutputStorage.get(referenceRunMetadata())).thenReturn(Optional.of(referenceAlignmentOutput()));
    }

    private void bothMetricsAvailable() {
        when(bamMetricsOutputStorage.get(eq(tumorRunMetadata()), any())).thenReturn(tumorMetricsOutput());
        when(bamMetricsOutputStorage.get(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput());
    }

    private void germlineCallingAvailable() {
        when(germlineCallerOutputStorage.get(eq(referenceRunMetadata()), any())).thenReturn(germlineCallerOutput());
    }
}