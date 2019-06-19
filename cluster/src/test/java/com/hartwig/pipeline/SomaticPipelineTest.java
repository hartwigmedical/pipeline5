package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestSamples.simpleReferenceSample;
import static com.hartwig.pipeline.testsupport.TestSamples.simpleTumorSample;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SetMetadata;
import com.hartwig.pipeline.metadata.SetMetadataApi;
import com.hartwig.pipeline.report.PatientReport;
import com.hartwig.pipeline.report.PatientReportProvider;
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
    private static final Sample TUMOR = simpleTumorSample();
    private static final Sample REFERENCE = simpleReferenceSample();
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
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(REFERENCE).build();
    private static final BamMetricsOutput TUMOR_BAM_METRICS_OUTPUT =
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(TUMOR).build();
    private static final AlignmentPair PAIR = AlignmentPair.of(tumorAlignmentOutput(), referenceAlignmentOutput());
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
    private SetMetadataApi setMetadataApi;

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
        setMetadataApi = mock(SetMetadataApi.class);
        when(setMetadataApi.get()).thenReturn(SetMetadata.of(SET_NAME, TUMOR, REFERENCE));
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PatientReport patientReport = PatientReportProvider.from(storage, ARGUMENTS).get();
        final Cleanup cleanup = mock(Cleanup.class);
        victim = new SomaticPipeline(alignmentOutputStorage,
                bamMetricsOutputStorage, setMetadataApi,
                patientReport,
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
        when(amber.run(PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_AMBER_OUTPUT);
    }

    @Test
    public void runsCobaltWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(cobalt.run(PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_COBALT_OUTPUT);
    }

    @Test
    public void runsSomaticCallerWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(somaticCaller.run(PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
    }

    @Test
    public void runsStructuralCallerWhenBothAlignmentsAvailable() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(structuralCaller.run(PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).contains(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
    }

    @Test
    public void runsPurpleWhenAllCallersSucceed() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        allCallersSucceed();
        when(purple.run(PAIR,
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
        when(cobalt.run(PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(somaticCaller.run(PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(amber.run(PAIR)).thenReturn(AmberOutput.builder().status(PipelineStatus.FAILED).build());
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
        when(purple.run(PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(PurpleOutput.builder().status(PipelineStatus.FAILED).build());
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
        verify(setMetadataApi, times(1)).complete(PipelineStatus.SUCCESS);
    }

    @Test
    public void notifiesSetMetadataApiOnFailedRun() {
        bothAlignmentsAvailable();
        allCallersSucceed();
        bothMetricsAvailable();
        when(purple.run(PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(PurpleOutput.builder().status(PipelineStatus.FAILED).build());
        victim.run();
        verify(setMetadataApi, times(1)).complete(PipelineStatus.FAILED);
    }

    private void purpleAndHealthCheckSucceed() {
        when(purple.run(PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_PURPLE_OUTPUT);
        when(healthChecker.run(PAIR,
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
        when(amber.run(PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        when(cobalt.run(PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(somaticCaller.run(PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(PAIR)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
    }

    private void bothAlignmentsAvailable() {
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(tumorAlignmentOutput()));
        when(alignmentOutputStorage.get(REFERENCE)).thenReturn(Optional.of(referenceAlignmentOutput()));
    }
}