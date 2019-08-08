package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestSamples.simpleReferenceSample;
import static com.hartwig.pipeline.testsupport.TestSamples.simpleTumorSample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
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
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.ImmutableSomaticRunMetadata;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.healthcheck.ImmutableHealthCheckOutput;
import com.hartwig.pipeline.tertiary.purple.ImmutablePurpleOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.junit.Before;
import org.junit.Test;

public class SomaticPipelineTest {

    private static final String SET_NAME = "test_set";
    private static final SingleSampleRunMetadata TUMOR = SingleSampleRunMetadata.builder()
            .type(SingleSampleRunMetadata.SampleType.TUMOR)
            .sampleId(simpleTumorSample().name())
            .sampleName(simpleTumorSample().name())
            .build();
    private static final SingleSampleRunMetadata REFERENCE = SingleSampleRunMetadata.builder()
            .type(SingleSampleRunMetadata.SampleType.REFERENCE)
            .sampleId(simpleReferenceSample().name())
            .sampleName(simpleReferenceSample().name())
            .build();
    private static final ImmutableSomaticRunMetadata SOMATIC_RUN_METADATA =
            SomaticRunMetadata.builder().runName(SET_NAME).maybeTumor(TUMOR).reference(REFERENCE).build();
    private static final CobaltOutput SUCCESSFUL_COBALT_OUTPUT = CobaltOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final AmberOutput SUCCESSFUL_AMBER_OUTPUT = AmberOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final StructuralCallerOutput SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT =
            StructuralCallerOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final SomaticCallerOutput SUCCESSFUL_SOMATIC_CALLER_OUTPUT =
            SomaticCallerOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final ImmutablePurpleOutput SUCCESSFUL_PURPLE_OUTPUT = PurpleOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final ImmutableHealthCheckOutput SUCCESSFUL_HEALTH_CHECK =
            HealthCheckOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final BamMetricsOutput REFERENCE_BAM_METRICS_OUTPUT =
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(REFERENCE.sampleName()).build();
    private static final BamMetricsOutput TUMOR_BAM_METRICS_OUTPUT =
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(TUMOR.sampleName()).build();
    private static final AlignmentPair PAIR = AlignmentPair.of(referenceAlignmentOutput(), tumorAlignmentOutput());
    public static final Arguments ARGUMENTS = Arguments.testDefaults();
    private AlignmentOutputStorage alignmentOutputStorage;
    private BamMetricsOutputStorage bamMetricsOutputStorage;
    private SomaticPipeline victim;
    private Amber amber;
    private Cobalt cobalt;
    private SomaticCaller somaticCaller;
    private StructuralCaller structuralCaller;
    private Purple purple;
    private HealthChecker healthChecker;
    private SomaticMetadataApi setMetadataApi;
    private Cleanup cleanup;

    @Before
    public void setUp() throws Exception {
        alignmentOutputStorage = mock(AlignmentOutputStorage.class);
        bamMetricsOutputStorage = mock(BamMetricsOutputStorage.class);
        amber = mock(Amber.class);
        cobalt = mock(Cobalt.class);
        somaticCaller = mock(SomaticCaller.class);
        structuralCaller = mock(StructuralCaller.class);
        purple = mock(Purple.class);
        healthChecker = mock(HealthChecker.class);
        setMetadataApi = mock(SomaticMetadataApi.class);
        when(setMetadataApi.get()).thenReturn(SOMATIC_RUN_METADATA);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        final FullSomaticResults fullSomaticResults = mock(FullSomaticResults.class);
        cleanup = mock(Cleanup.class);
        victim = new SomaticPipeline(alignmentOutputStorage,
                bamMetricsOutputStorage,
                setMetadataApi,
                pipelineResults,
                fullSomaticResults,
                cleanup,
                amber,
                cobalt,
                somaticCaller,
                structuralCaller,
                purple,
                healthChecker,
                Executors.newSingleThreadExecutor());
    }

    @Test(expected = IllegalStateException.class)
    public void failsIfTumorAlignmentNotAvailable() {
        victim.run();
    }

    @Test(expected = IllegalStateException.class)
    public void failsIfReferenceAlignmentNotAvailable() {
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(tumorAlignmentOutput()));
        victim.run();
    }

    @Test
    public void runsAmberWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(amber.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_AMBER_OUTPUT);
    }

    @Test
    public void runsCobaltWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(cobalt.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_COBALT_OUTPUT);
    }

    @Test
    public void runsSomaticCallerWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(somaticCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
    }

    @Test
    public void runsStructuralCallerWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(structuralCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
    }

    @Test
    public void runsPurpleWhenAllCallersSucceed() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        allCallersSucceed();
        when(purple.run(SOMATIC_RUN_METADATA,
                PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_PURPLE_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_PURPLE_OUTPUT);
    }

    @Test
    public void doesNotRunPurpleIfAnyCallersFail() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(cobalt.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(somaticCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(amber.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(AmberOutput.builder().status(PipelineStatus.FAILED).build());
        PipelineState state = victim.run();
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
        verifyZeroInteractions(purple);
    }

    @Test
    public void runsHealthCheckWhenPurpleSucceeds() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        purpleAndHealthCheckSucceed();
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_HEALTH_CHECK);
    }

    @Test
    public void doesNotRunHealthCheckWhenPurpleFails() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        failPurple();
        PipelineState state = victim.run();
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
        verifyZeroInteractions(healthChecker);
    }

    @Test
    public void notifiesSetMetadataApiOnSuccessfulRun() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        purpleAndHealthCheckSucceed();
        victim.run();
        verify(setMetadataApi, times(1)).complete(PipelineStatus.SUCCESS, SOMATIC_RUN_METADATA);
    }

    @Test
    public void notifiesSetMetadataApiOnFailedRun() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        failPurple();
        victim.run();
        verify(setMetadataApi, times(1)).complete(PipelineStatus.FAILED, SOMATIC_RUN_METADATA);
    }

    @Test
    public void runsCleanupOnSuccessfulRun() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        purpleAndHealthCheckSucceed();
        victim.run();
        verify(cleanup, times(1)).run(SOMATIC_RUN_METADATA);
    }

    @Test
    public void doesNotRunCleanupOnFailedRun() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        failPurple();
        victim.run();
        verify(cleanup, never()).run(SOMATIC_RUN_METADATA);
    }

    @Test
    public void doesNotRunCleanupOnFailedTransfer() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        purpleAndHealthCheckSucceed();
        doThrow(new NullPointerException()).when(setMetadataApi).complete(PipelineStatus.SUCCESS, SOMATIC_RUN_METADATA);
        try {
            victim.run();
        } catch (Exception e) {
            // continue
        }
        verify(cleanup, never()).run(SOMATIC_RUN_METADATA);
    }

    @Test
    public void onlyDoesTransferAndCleanupIfSingleSampleRun() {
        when(alignmentOutputStorage.get(REFERENCE)).thenReturn(Optional.of(referenceAlignmentOutput()));
        when(setMetadataApi.get()).thenReturn(SomaticRunMetadata.builder().from(SOMATIC_RUN_METADATA).maybeTumor(Optional.empty()).build());
        victim.run();
        verifyZeroInteractions(amber, cobalt, purple, structuralCaller, somaticCaller, healthChecker);
    }

    private void failPurple() {
        when(purple.run(SOMATIC_RUN_METADATA,
                PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(PurpleOutput.builder().status(PipelineStatus.FAILED).build());
    }

    private void purpleAndHealthCheckSucceed() {
        when(purple.run(SOMATIC_RUN_METADATA,
                PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_PURPLE_OUTPUT);
        when(healthChecker.run(SOMATIC_RUN_METADATA,
                PAIR,
                TUMOR_BAM_METRICS_OUTPUT,
                REFERENCE_BAM_METRICS_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT,
                SUCCESSFUL_PURPLE_OUTPUT)).thenReturn(SUCCESSFUL_HEALTH_CHECK);
    }

    private void bothMetricsAvailable() {
        when(bamMetricsOutputStorage.get(TUMOR)).thenReturn(TUMOR_BAM_METRICS_OUTPUT);
        when(bamMetricsOutputStorage.get(REFERENCE)).thenReturn(REFERENCE_BAM_METRICS_OUTPUT);
    }

    private void allCallersSucceed() {
        when(amber.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        when(cobalt.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(somaticCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(SOMATIC_RUN_METADATA, PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
    }

    private void bothAlignmentsAvailable() {
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(tumorAlignmentOutput()));
        when(alignmentOutputStorage.get(REFERENCE)).thenReturn(Optional.of(referenceAlignmentOutput()));
    }
}