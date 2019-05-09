package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.Executors;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.bammetrics.BamMetrics;
import com.hartwig.pipeline.bammetrics.BamMetricsOutput;
import com.hartwig.pipeline.bammetrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.bammetrics.ImmutableBamMetricsOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.germline.ImmutableGermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.ImmutableSomaticCallerOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.ImmutableStructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.amber.ImmutableAmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cobalt.ImmutableCobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.healthcheck.ImmutableHealthCheckOutput;
import com.hartwig.pipeline.tertiary.purple.ImmutablePurpleOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;

public class PatientReportPipelineTest {

    private static final GermlineCallerOutput SUCCESSFUL_GERMLINE_OUTPUT = GermlineCallerOutput.builder().status(JobStatus.SUCCESS).build();
    private static final BamMetricsOutput SUCCESSFUL_BAM_METRICS = BamMetricsOutput.builder().status(JobStatus.SUCCESS).build();
    private static final BamMetricsOutput MATE_BAM_METRICS = BamMetricsOutput.builder().status(JobStatus.SUCCESS).build();
    private static final CobaltOutput SUCCESSFUL_COBALT_OUTPUT = CobaltOutput.builder().status(JobStatus.SUCCESS).build();
    private static final AmberOutput SUCCESSFUL_AMBER_OUTPUT = AmberOutput.builder().status(JobStatus.SUCCESS).build();
    private static final StructuralCallerOutput SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT =
            StructuralCallerOutput.builder().status(JobStatus.SUCCESS).build();
    private static final SomaticCallerOutput SUCCESSFUL_SOMATIC_CALLER_OUTPUT =
            SomaticCallerOutput.builder().status(JobStatus.SUCCESS).build();
    private static final ImmutablePurpleOutput SUCCESSFUL_PURPLE_OUTPUT = PurpleOutput.builder().status(JobStatus.SUCCESS).build();
    private static final ImmutableHealthCheckOutput SUCCESSFUL_HEALTH_CHECK = HealthCheckOutput.builder().status(JobStatus.SUCCESS).build();
    private PatientReportPipeline victim;
    private Aligner aligner;
    private BamMetrics bamMetrics;
    private GermlineCaller germlineCaller;
    private SomaticCaller somaticCaller;
    private StructuralCaller structuralCaller;
    private Amber amber;
    private Cobalt cobalt;
    private Purple purple;
    private HealthChecker healthChecker;
    private AlignmentOutputStorage alignmentOutputStorage;
    private BamMetricsOutputStorage bamMetricsOutputStorage;
    private static final Sample REFERENCE = Sample.builder("", "TESTR").type(Sample.Type.REFERENCE).build();
    private static final Sample TUMOR = Sample.builder("", "TESTT").type(Sample.Type.TUMOR).build();
    private static final ImmutableAlignmentOutput SUCCESSFUL_ALIGNMENT_OUTPUT =
            AlignmentOutput.builder().status(JobStatus.SUCCESS).sample(REFERENCE).build();
    private static final ImmutableAlignmentOutput MATE_ALIGNMENT_OUTPUT =
            AlignmentOutput.builder().status(JobStatus.SUCCESS).sample(TUMOR).build();
    private static final AlignmentPair ALIGNMENT_PAIR = AlignmentPair.of(SUCCESSFUL_ALIGNMENT_OUTPUT, MATE_ALIGNMENT_OUTPUT);

    @Before
    public void setUp() throws Exception {
        aligner = mock(Aligner.class);
        bamMetrics = mock(BamMetrics.class);
        germlineCaller = mock(GermlineCaller.class);
        somaticCaller = mock(SomaticCaller.class);
        structuralCaller = mock(StructuralCaller.class);
        amber = mock(Amber.class);
        cobalt = mock(Cobalt.class);
        purple = mock(Purple.class);
        healthChecker = mock(HealthChecker.class);
        alignmentOutputStorage = mock(AlignmentOutputStorage.class);
        bamMetricsOutputStorage = mock(BamMetricsOutputStorage.class);
        victim = new PatientReportPipeline(aligner,
                bamMetrics,
                germlineCaller,
                somaticCaller,
                structuralCaller,
                amber,
                cobalt,
                purple,
                healthChecker,
                alignmentOutputStorage,
                bamMetricsOutputStorage,
                Executors.newSingleThreadExecutor());
    }

    @Test
    public void returnsFailedPipelineRunWhenAlignerStageFail() throws Exception {
        ImmutableAlignmentOutput alignmentOutput =
                AlignmentOutput.builder().status(JobStatus.FAILED).sample(TestSamples.simpleReferenceSample()).build();
        when(aligner.run()).thenReturn(alignmentOutput);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(alignmentOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenMetricsStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        ImmutableBamMetricsOutput bamMetricsOutput = BamMetricsOutput.builder().status(JobStatus.FAILED).build();
        when(bamMetrics.run(any())).thenReturn(bamMetricsOutput);
        when(germlineCaller.run(any())).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT, bamMetricsOutput, SUCCESSFUL_GERMLINE_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenGermlineStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        ImmutableGermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(JobStatus.FAILED).build();
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(germlineCallerOutput);
        PipelineState state = victim.run();
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT, SUCCESSFUL_BAM_METRICS, germlineCallerOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenOneOfThePairSampleCallersStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(MATE_ALIGNMENT_OUTPUT));
        ImmutableSomaticCallerOutput somaticCallerOutput = SomaticCallerOutput.builder().status(JobStatus.FAILED).build();
        when(somaticCaller.run(ALIGNMENT_PAIR)).thenReturn(somaticCallerOutput);
        when(structuralCaller.run(ALIGNMENT_PAIR, SUCCESSFUL_BAM_METRICS)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(cobalt.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(amber.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        PipelineState state = victim.run();
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                somaticCallerOutput,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenPurpleStageFail() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(MATE_ALIGNMENT_OUTPUT));
        when(somaticCaller.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(ALIGNMENT_PAIR, SUCCESSFUL_BAM_METRICS)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(cobalt.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(amber.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        ImmutablePurpleOutput purpleOutput = PurpleOutput.builder().status(JobStatus.FAILED).build();
        when(purple.run(ALIGNMENT_PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(purpleOutput);
        PipelineState state = victim.run();
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT,
                purpleOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenHealthCheckerFails() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(MATE_ALIGNMENT_OUTPUT));
        when(somaticCaller.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(ALIGNMENT_PAIR, SUCCESSFUL_BAM_METRICS)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(cobalt.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(amber.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        when(purple.run(ALIGNMENT_PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_PURPLE_OUTPUT);
        when(bamMetricsOutputStorage.get(TUMOR)).thenReturn(MATE_BAM_METRICS);
        ImmutableHealthCheckOutput healthCheckOutput = HealthCheckOutput.builder().status(JobStatus.FAILED).build();
        when(healthChecker.run(ALIGNMENT_PAIR,
                SUCCESSFUL_BAM_METRICS,
                MATE_BAM_METRICS,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_PURPLE_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(healthCheckOutput);
        PipelineState state = victim.run();
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT,
                SUCCESSFUL_PURPLE_OUTPUT,
                healthCheckOutput);
    }

    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceed() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(alignmentOutputStorage.get(TUMOR)).thenReturn(Optional.of(MATE_ALIGNMENT_OUTPUT));
        when(somaticCaller.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_SOMATIC_CALLER_OUTPUT);
        when(structuralCaller.run(ALIGNMENT_PAIR, SUCCESSFUL_BAM_METRICS)).thenReturn(SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT);
        when(cobalt.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_COBALT_OUTPUT);
        when(amber.run(ALIGNMENT_PAIR)).thenReturn(SUCCESSFUL_AMBER_OUTPUT);
        when(purple.run(ALIGNMENT_PAIR,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_PURPLE_OUTPUT);
        when(bamMetricsOutputStorage.get(TUMOR)).thenReturn(MATE_BAM_METRICS);
        when(healthChecker.run(ALIGNMENT_PAIR,
                SUCCESSFUL_BAM_METRICS,
                MATE_BAM_METRICS,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_PURPLE_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT)).thenReturn(SUCCESSFUL_HEALTH_CHECK);
        PipelineState state = victim.run();
        assertSucceeded(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SOMATIC_CALLER_OUTPUT,
                SUCCESSFUL_STRUCTURAL_CALLER_OUTPUT,
                SUCCESSFUL_COBALT_OUTPUT,
                SUCCESSFUL_AMBER_OUTPUT,
                SUCCESSFUL_PURPLE_OUTPUT,
                SUCCESSFUL_HEALTH_CHECK);
    }

    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceedNoMate() throws Exception {
        when(aligner.run()).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(bamMetrics.run(any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        PipelineState state = victim.run();
        assertSucceeded(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT);
    }

    private void assertFailed(final PipelineState runOutput) throws Exception {
        assertThat(runOutput.status()).isEqualTo(JobStatus.FAILED);
    }

    private void assertSucceeded(final PipelineState runOutput) throws Exception {
        assertThat(runOutput.status()).isEqualTo(JobStatus.SUCCESS);
    }
}