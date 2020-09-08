package com.hartwig.pipeline;

public class SomaticPipelineTest {

  /*  private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private AlignmentOutputStorage alignmentOutputStorage;
    private OutputStorage<BamMetricsOutput, SingleSampleRunMetadata> bamMetricsOutputStorage;
    private SomaticPipeline victim;
    private StructuralCaller structuralCaller;
    private SomaticMetadataApi setMetadataApi;
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
        when(storage.get(ARGUMENTS.outputBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        final FullSomaticResults fullSomaticResults = mock(FullSomaticResults.class);
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
                sageOutput(),
                structuralCallerOutput(),
                purpleOutput(),
                healthCheckerOutput(),
                linxOutput(),
                bachelorOutput(),
                chordOutput());
    }

    @Test
    public void doesNotRunPurpleIfAnyCallersFail() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        SomaticCallerOutput failSomatic = SomaticCallerOutput.builder(SageCaller.NAMESPACE).status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(failSomatic)
                .thenReturn(structuralCallerOutput());
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                failSomatic,
                structuralCallerOutput());
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void doesNotRunHealthCheckWhenPurpleFails() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        PurpleOutput failPurple = PurpleOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(failPurple);
        PipelineState state = victim.run();
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(cobaltOutput(),
                amberOutput(),
                sageOutput(),
                structuralCallerOutput(),
                failPurple);
        assertThat(state.status()).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void failsRunOnQcFailure() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(purpleOutput())
                .thenReturn(HealthCheckOutput.builder().from(healthCheckerOutput()).status(PipelineStatus.QC_FAILED).build())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput())
                .thenReturn(chordOutput());
        PipelineState state = victim.run();
        assertThat(state.status()).isEqualTo(PipelineStatus.QC_FAILED);
    }

    @Test
    public void skipsStructuralCallerIfSingleSampleRun() {
        when(alignmentOutputStorage.get(referenceRunMetadata())).thenReturn(Optional.of(referenceAlignmentOutput()));
        when(setMetadataApi.get()).thenReturn(SomaticRunMetadata.builder()
                .from(defaultSomaticRunMetadata())
                .maybeTumor(Optional.empty())
                .build());
        victim.run();
        verifyZeroInteractions(stageRunner, structuralCaller);
    }

    private void successfulRun() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        germlineCallingAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
                .thenReturn(purpleOutput())
                .thenReturn(healthCheckerOutput())
                .thenReturn(linxOutput())
                .thenReturn(bachelorOutput())
                .thenReturn(chordOutput());
    }

    private void failedRun() {
        bothAlignmentsAvailable();
        bothMetricsAvailable();
        when(stageRunner.run(eq(defaultSomaticRunMetadata()), any())).thenReturn(amberOutput())
                .thenReturn(cobaltOutput())
                .thenReturn(sageOutput())
                .thenReturn(structuralCallerOutput())
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
    }*/
}