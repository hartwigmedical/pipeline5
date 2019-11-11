package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.flagstatOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.germlineCallerOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;
import static com.hartwig.pipeline.testsupport.TestInputs.referenceSample;
import static com.hartwig.pipeline.testsupport.TestInputs.snpGenotypeOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorAlignmentOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorMetricsOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.tumorRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.stages.StageRunner;

import org.junit.Before;
import org.junit.Test;

public class SingleSamplePipelineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SingleSamplePipeline victim;
    private Aligner aligner;
    private SingleSampleEventListener eventListener;
    private StageRunner<SingleSampleRunMetadata> stageRunner;
    private PipelineResults pipelineResults;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        aligner = mock(Aligner.class);
        eventListener = mock(SingleSampleEventListener.class);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        stageRunner = mock(StageRunner.class);
        victim = new SingleSamplePipeline(eventListener,
                stageRunner,
                aligner,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                ARGUMENTS);
    }

    @Test
    public void returnsFailedPipelineRunWhenAlignerStageFail() throws Exception {
        AlignmentOutput alignmentOutput = AlignmentOutput.builder().status(PipelineStatus.FAILED).sample(referenceSample()).build();
        when(aligner.run(referenceRunMetadata())).thenReturn(alignmentOutput);
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(alignmentOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenFlagstatStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        FlagstatOutput flagstatOutput = FlagstatOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput)
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenSnpGenotypeStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        SnpGenotypeOutput snpGenotypeOutput = SnpGenotypeOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput)
                .thenReturn(flagstatOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput,
                flagstatOutput());
    }

    @Test
    public void returnsFailedPipelineRunWhenMetricsStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        BamMetricsOutput bamMetricsOutput = BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample(referenceSample()).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(bamMetricsOutput)
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                bamMetricsOutput,
                snpGenotypeOutput(),
                flagstatOutput());
    }

    @Test
    public void returnsFailedPipelineRunWhenGermlineStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        GermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(germlineCallerOutput);
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput,
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput());
    }

    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceed() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertSucceeded(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput());
    }

    @Test
    public void skipsGermlineForTumorSamples() throws Exception {
        when(aligner.run(tumorRunMetadata())).thenReturn(tumorAlignmentOutput());
        when(stageRunner.run(eq(tumorRunMetadata()), any())).thenReturn(tumorMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput());
        PipelineState runOutput = victim.run(tumorRunMetadata());
        assertSucceeded(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(tumorAlignmentOutput(),
                tumorMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput());
    }

    @Test
    public void addsCompleteStagesToFinalPatientReport() throws Exception {

        TestReportComponent alignerComponent = new TestReportComponent();
        TestReportComponent metricsComponent = new TestReportComponent();
        TestReportComponent germlineComponent = new TestReportComponent();
        TestReportComponent snpgenotypeComponent = new TestReportComponent();
        TestReportComponent flagstatComponent = new TestReportComponent();

        AlignmentOutput alignmentWithReportComponents =
                AlignmentOutput.builder().from(referenceAlignmentOutput()).addReportComponents(alignerComponent).build();
        when(aligner.run(referenceRunMetadata())).thenReturn(alignmentWithReportComponents);
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(BamMetricsOutput.builder()
                .from(alignmentWithReportComponents)
                .addReportComponents(metricsComponent)
                .sample(referenceSample())
                .build())
                .thenReturn(SnpGenotypeOutput.builder().from(snpGenotypeOutput()).addReportComponents(snpgenotypeComponent).build())
                .thenReturn(FlagstatOutput.builder().from(flagstatOutput()).addReportComponents(flagstatComponent).build())
                .thenReturn(GermlineCallerOutput.builder().from(germlineCallerOutput()).addReportComponents(germlineComponent).build());
        victim.run(referenceRunMetadata());
        assertThat(alignerComponent.isAdded()).isTrue();
        assertThat(metricsComponent.isAdded()).isTrue();
        assertThat(germlineComponent.isAdded()).isTrue();
        assertThat(snpgenotypeComponent.isAdded()).isTrue();
        assertThat(flagstatComponent.isAdded()).isTrue();
    }

    @Test
    public void notifiesMetadataApiWhenAlignmentComplete() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        PipelineState result = victim.run(referenceRunMetadata());
        verify(eventListener, times(1)).alignmentComplete(result);
    }

    @Test
    public void notifiesMetadataApiWhenPipelineComplete() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState result = victim.run(referenceRunMetadata());
        verify(eventListener, times(1)).alignmentComplete(result);
    }

    private void assertFailed(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(PipelineStatus.FAILED);
    }

    private void assertSucceeded(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    private class TestReportComponent implements ReportComponent {

        private boolean isAdded;

        @Override
        public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
            isAdded = true;
        }

        boolean isAdded() {
            return isAdded;
        }
    }
}