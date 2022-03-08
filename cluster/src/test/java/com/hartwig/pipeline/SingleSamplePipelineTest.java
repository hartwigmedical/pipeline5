package com.hartwig.pipeline;

import static com.hartwig.pipeline.testsupport.TestInputs.cramOutput;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.cram.CramOutput;
import com.hartwig.pipeline.cram2bam.Cram2Bam;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SingleSamplePipelineTest {

    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SingleSamplePipeline victim;
    private Aligner aligner;
    private SingleSampleEventListener eventListener;
    private StageRunner<SingleSampleRunMetadata> stageRunner;
    private PipelineResults pipelineResults;
    private BlockingQueue<BamMetricsOutput> metricsOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<FlagstatOutput> flagstatOutputQueue = new ArrayBlockingQueue<>(1);
    private BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue = new ArrayBlockingQueue<>(1);

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        aligner = mock(Aligner.class);
        eventListener = mock(SingleSampleEventListener.class);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.outputBucket())).thenReturn(reportBucket);
        stageRunner = mock(StageRunner.class);
        pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        initialiseVictim(false);
    }

    private void initialiseVictim(final boolean standalone) {
        victim = new SingleSamplePipeline(eventListener,
                stageRunner,
                aligner,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                standalone,
                ARGUMENTS,
                new NoopPersistedDataset(),
                metricsOutputQueue,
                flagstatOutputQueue,
                germlineCallerOutputQueue);
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
        FlagstatOutput flagstatOutput = FlagstatOutput.builder().sample(referenceSample()).status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput)
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput,
                cramOutput());
    }

    @Test
    public void returnsFailedPipelineRunWhenSnpGenotypeStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        SnpGenotypeOutput snpGenotypeOutput = SnpGenotypeOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput)
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput,
                flagstatOutput(),
                cramOutput());
    }

    @Test
    public void returnsFailedPipelineRunWhenMetricsStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        BamMetricsOutput bamMetricsOutput = BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample(referenceSample()).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(bamMetricsOutput)
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                bamMetricsOutput,
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput());
    }

    @Test
    public void returnsFailedPipelineRunWhenCramStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        CramOutput cramOutput = CramOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput)
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenGermlineStageFail() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        GermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput);
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput,
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput());
    }

    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceed() throws Exception {
        setupReferenceSamplePipeline();
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertSucceeded(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput());
        assertThat(metricsOutputQueue.poll()).isNotNull();
        assertThat(flagstatOutputQueue.poll()).isNotNull();
        assertThat(germlineCallerOutputQueue.poll()).isNotNull();
    }

    @Test
    public void skipsGermlineForTumorSamples() throws Exception {
        when(aligner.run(tumorRunMetadata())).thenReturn(tumorAlignmentOutput());
        when(stageRunner.run(eq(tumorRunMetadata()), any())).thenReturn(tumorMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput());
        PipelineState runOutput = victim.run(tumorRunMetadata());
        assertSucceeded(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(tumorAlignmentOutput(),
                tumorMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput());
    }

    @Test
    public void addsCompleteStagesToFinalPatientReport() throws Exception {

        TestReportComponent alignerComponent = new TestReportComponent();
        TestReportComponent metricsComponent = new TestReportComponent();
        TestReportComponent germlineComponent = new TestReportComponent();
        TestReportComponent snpgenotypeComponent = new TestReportComponent();
        TestReportComponent flagstatComponent = new TestReportComponent();
        TestReportComponent cramComponent = new TestReportComponent();

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
                .thenReturn(CramOutput.builder().from(cramOutput()).addReportComponents(cramComponent).build())
                .thenReturn(GermlineCallerOutput.builder().from(germlineCallerOutput()).addReportComponents(germlineComponent).build());
        victim.run(referenceRunMetadata());
        assertThat(alignerComponent.isAdded()).isTrue();
        assertThat(metricsComponent.isAdded()).isTrue();
        assertThat(germlineComponent.isAdded()).isTrue();
        assertThat(snpgenotypeComponent.isAdded()).isTrue();
        assertThat(flagstatComponent.isAdded()).isTrue();
        assertThat(cramComponent.isAdded()).isTrue();
    }

    @Test
    public void notifiesMetadataApiWhenAlignmentComplete() throws Exception {
        setupReferenceSamplePipeline();
        PipelineState result = victim.run(referenceRunMetadata());
        ArgumentCaptor<AlignmentOutput> stateCaptor = ArgumentCaptor.forClass(AlignmentOutput.class);
        verify(eventListener, times(1)).alignmentComplete(stateCaptor.capture());
        AlignmentOutput alignmentResult = stateCaptor.getValue();
        assertThat(alignmentResult).isNotSameAs(result);
        assertThat(alignmentResult.status()).isEqualTo(PipelineStatus.SUCCESS);
        assertThat(alignmentResult).isEqualTo(referenceAlignmentOutput());
    }

    public void setupReferenceSamplePipeline() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
    }

    @Test
    public void notifiesMetadataApiWhenPipelineComplete() throws Exception {
        setupReferenceSamplePipeline();
        PipelineState result = victim.run(referenceRunMetadata());
        verify(eventListener, times(1)).complete(result);
    }

    @Test
    public void passesFalseToReportCompositionWhenNotRunInStandaloneMode() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        when(pipelineResults.add(any())).thenAnswer(i -> i.getArguments()[0]);
        setupReferenceSamplePipeline();
        initialiseVictim(false);
        victim.run(referenceRunMetadata());
        verify(pipelineResults).compose(any(), eq(false), any());
    }

    @Test
    public void passesTrueToReportCompositionWhenRunInStandaloneMode() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        when(pipelineResults.add(any())).thenAnswer(i -> i.getArguments()[0]);
        setupReferenceSamplePipeline();
        initialiseVictim(true);
        victim.run(referenceRunMetadata());
        verify(pipelineResults).compose(any(), eq(true), any());
    }

    @Test
    public void doesNotComposeReportIfStatusIsFailed() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        AlignmentOutput alignmentOutput = AlignmentOutput.builder().status(PipelineStatus.FAILED).sample(referenceSample()).build();
        when(aligner.run(referenceRunMetadata())).thenReturn(alignmentOutput);
        victim.run(referenceRunMetadata());
        verify(pipelineResults, never()).compose(any(), eq(true), any());
    }

    @Test
    public void clearsOutExistingStagingFlag() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        when(pipelineResults.add(any())).thenAnswer(i -> i.getArguments()[0]);
        setupReferenceSamplePipeline();
        initialiseVictim(false);
        victim.run(referenceRunMetadata());
        verify(pipelineResults).clearOldState(any(Arguments.class), eq(referenceRunMetadata()));
    }

    @Test
    public void convertsToBamIfRunFromCram() throws Exception {
        AlignmentOutput alignmentOutput = AlignmentOutput.builder()
                .status(PipelineStatus.SUCCESS)
                .sample(referenceSample())
                .maybeAlignments(GoogleStorageLocation.of("run-reference-test/aligner", "results/reference.cram"))
                .build();
        when(aligner.run(referenceRunMetadata())).thenReturn(alignmentOutput);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Stage<AlignmentOutput, SingleSampleRunMetadata>> stageArgumentCaptor = ArgumentCaptor.forClass(Stage.class);
        AlignmentOutput cram2Bam = AlignmentOutput.builder()
                .status(PipelineStatus.FAILED)
                .sample(referenceSample())
                .maybeAlignments(GoogleStorageLocation.of("run-reference-test/cram2bam", "results/reference.bam"))
                .build();
        when(stageRunner.run(eq(referenceRunMetadata()), stageArgumentCaptor.capture())).thenReturn(cram2Bam);
        PipelineState runOutput = victim.run(referenceRunMetadata());
        Stage<AlignmentOutput, SingleSampleRunMetadata> cram2bamStage = stageArgumentCaptor.getValue();
        assertThat(cram2bamStage).isInstanceOf(Cram2Bam.class);
        assertThat(runOutput.stageOutputs()).contains(cram2Bam);
    }

    private void assertFailed(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(PipelineStatus.FAILED);
    }

    private void assertSucceeded(final PipelineState runOutput) {
        assertThat(runOutput.status()).isEqualTo(PipelineStatus.SUCCESS);
    }

    private static class TestReportComponent implements ReportComponent {

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