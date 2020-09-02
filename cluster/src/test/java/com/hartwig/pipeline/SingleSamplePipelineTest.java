package com.hartwig.pipeline;

public class SingleSamplePipelineTest {

/*    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private SingleSamplePipeline victim;
    private BwaAligner aligner;
    private SingleSampleEventListener eventListener;
    private StageRunner<SingleSampleRunMetadata> stageRunner;
    private PipelineResults pipelineResults;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        aligner = mock(BwaAligner.class);
        eventListener = mock(SingleSampleEventListener.class);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.outputBucket())).thenReturn(reportBucket);
        stageRunner = mock(StageRunner.class);
        pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        initialiseVictim(false);
    }

    private void initialiseVictim(boolean standalone) {
        victim = new SingleSamplePipeline(eventListener,
                stageRunner,
                aligner,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                standalone,
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
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState runOutput = victim.run(referenceRunMetadata());
        assertSucceeded(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(referenceAlignmentOutput(),
                germlineCallerOutput(),
                referenceMetricsOutput(),
                snpGenotypeOutput(),
                flagstatOutput(),
                cramOutput());
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
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        PipelineState result = victim.run(referenceRunMetadata());
        ArgumentCaptor<PipelineState> stateCaptor = ArgumentCaptor.forClass(PipelineState.class);
        verify(eventListener, times(1)).alignmentComplete(stateCaptor.capture());
        PipelineState alignmentResult = stateCaptor.getValue();
        assertThat(alignmentResult).isNotSameAs(result);
        assertThat(alignmentResult.status()).isEqualTo(PipelineStatus.SUCCESS);
        assertThat(alignmentResult.stageOutputs().size()).isEqualTo(1);
        assertThat(alignmentResult.stageOutputs().get(0).name()).isEqualTo(referenceAlignmentOutput().name());
    }

    @Test
    public void notifiesMetadataApiWhenPipelineComplete() throws Exception {
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        when(stageRunner.run(eq(referenceRunMetadata()), any())).thenReturn(referenceMetricsOutput())
                .thenReturn(snpGenotypeOutput())
                .thenReturn(flagstatOutput())
                .thenReturn(cramOutput())
                .thenReturn(germlineCallerOutput());
        PipelineState result = victim.run(referenceRunMetadata());
        verify(eventListener, times(1)).complete(result);
    }

    @Test
    public void passesFalseToReportCompositionWhenNotRunInStandaloneMode() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        when(pipelineResults.add(any())).thenAnswer(i -> i.getArguments()[0]);
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        initialiseVictim(false);
        victim.run(referenceRunMetadata());
        verify(pipelineResults).compose(any(), eq(false), any());
    }

    @Test
    public void passesTrueToReportCompositionWhenRunInStandaloneMode() throws Exception {
        pipelineResults = mock(PipelineResults.class);
        when(pipelineResults.add(any())).thenAnswer(i -> i.getArguments()[0]);
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
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
        when(aligner.run(referenceRunMetadata())).thenReturn(referenceAlignmentOutput());
        initialiseVictim(false);
        victim.run(referenceRunMetadata());
        verify(pipelineResults).clearOldState(any(Arguments.class), eq(referenceRunMetadata()));
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
    }*/
}