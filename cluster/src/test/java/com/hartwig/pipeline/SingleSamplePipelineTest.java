package com.hartwig.pipeline;

import org.junit.Ignore;

@Ignore
public class SingleSamplePipelineTest {

   /* private static final String REFERENCE = "TESTR";
    private static final SingleSampleRunMetadata REFERENCE_METADATA = SingleSampleRunMetadata.builder()
            .sampleId(REFERENCE)
            .sampleName(REFERENCE)
            .type(SingleSampleRunMetadata.SampleType.REFERENCE)
            .build();
    private static final SnpGenotypeOutput SUCCESSFUL_SNPGENOTYPE_OUTPUT =
            SnpGenotypeOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final GermlineCallerOutput SUCCESSFUL_GERMLINE_OUTPUT =
            GermlineCallerOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final ImmutableFlagstatOutput SUCCESSFUL_FLAGSTAT_OUTPUT =
            FlagstatOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final BamMetricsOutput SUCCESSFUL_BAM_METRICS =
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(REFERENCE).build();
    private static final ImmutableAlignmentOutput SUCCESSFUL_ALIGNMENT_OUTPUT = AlignmentOutput.builder()
            .status(PipelineStatus.SUCCESS)
            .sample(REFERENCE)
            .maybeFinalBamLocation(GoogleStorageLocation.of("run-testr/aligner", "testr.bam"))
            .build();
    public static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final String TUMOR = "TESTT";
    private SingleSamplePipeline victim;
    private Aligner aligner;
    private GermlineCaller germlineCaller;
    private Flagstat flagstat;
    private SingleSampleEventListener eventListener;
    private StageRunner<SingleSampleRunMetadata> stageRunner;

    @Before
    public void setUp() throws Exception {
        aligner = mock(DataprocAligner.class);
        germlineCaller = mock(GermlineCaller.class);
        flagstat = mock(Flagstat.class);
        eventListener = mock(SingleSampleEventListener.class);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PipelineResultsProvider.from(storage, ARGUMENTS, "test").get();
        //noinspection unchecked
        stageRunner = mock(StageRunner.class);
        victim = new SingleSamplePipeline(eventListener,
                stageRunner,
                aligner, pipelineResults,
                Executors.newSingleThreadExecutor(),
                ARGUMENTS);
    }

    @Test
    public void returnsFailedPipelineRunWhenAlignerStageFail() throws Exception {
        ImmutableAlignmentOutput alignmentOutput =
                AlignmentOutput.builder().status(PipelineStatus.FAILED).sample(TestSamples.simpleReferenceSample().name()).build();
        when(aligner.run(REFERENCE_METADATA)).thenReturn(alignmentOutput);
        PipelineState runOutput = victim.run(REFERENCE_METADATA);
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(alignmentOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenFlagstatStageFail() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        FlagstatOutput flagstatOutput = FlagstatOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_BAM_METRICS);
   //     when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
     //   when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(flagstatOutput);
        PipelineState runOutput = victim.run(REFERENCE_METADATA);
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                flagstatOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenSnpGenotypeStageFail() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        ImmutableSnpGenotypeOutput snpGenotypeOutput = SnpGenotypeOutput.builder().status(PipelineStatus.FAILED).build();
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_BAM_METRICS);
//        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
  //      when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run(REFERENCE_METADATA);
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                snpGenotypeOutput,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenMetricsStageFail() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        ImmutableBamMetricsOutput bamMetricsOutput = BamMetricsOutput.builder().status(PipelineStatus.FAILED).sample(REFERENCE).build();
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(bamMetricsOutput);
  //      when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
  //      when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run(REFERENCE_METADATA);
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_GERMLINE_OUTPUT,
                bamMetricsOutput,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void returnsFailedPipelineRunWhenGermlineStageFail() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_BAM_METRICS);
        ImmutableGermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(PipelineStatus.FAILED).build();
   //     when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(germlineCallerOutput);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
  //      when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run(REFERENCE_METADATA);
        assertFailed(state);
        assertThat(state.stageOutputs()).containsExactly(SUCCESSFUL_ALIGNMENT_OUTPUT,
                germlineCallerOutput,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void returnsSuccessfulPipelineRunAllStagesSucceed() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_BAM_METRICS);
  //      when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
   //     when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run(REFERENCE_METADATA);
        assertSucceeded(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(SUCCESSFUL_ALIGNMENT_OUTPUT,
                SUCCESSFUL_BAM_METRICS,
                SUCCESSFUL_GERMLINE_OUTPUT,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void skipsGermlineForTumorSamples() throws Exception {
        ImmutableSingleSampleRunMetadata tumorMetadata =
                SingleSampleRunMetadata.builder().sampleId(TUMOR).type(SingleSampleRunMetadata.SampleType.TUMOR).build();
        AlignmentOutput tumorAlignment = AlignmentOutput.builder().from(SUCCESSFUL_ALIGNMENT_OUTPUT).sample(TUMOR).build();
        when(aligner.run(tumorMetadata)).thenReturn(tumorAlignment);
        BamMetricsOutput tumorMetrics = BamMetricsOutput.builder().from(SUCCESSFUL_BAM_METRICS).sample(TUMOR).build();
        when(stageRunner.run(eq(tumorMetadata), Mockito.<BamMetrics>any())).thenReturn(tumorMetrics);
    //    when(germlineCaller.run(tumorMetadata, tumorAlignment)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), Mockito.<SnpGenotype>any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(tumorMetadata, tumorAlignment)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run(tumorMetadata);
        assertSucceeded(state);
        assertThat(state.stageOutputs()).containsExactlyInAnyOrder(tumorAlignment,
                tumorMetrics,
                SUCCESSFUL_SNPGENOTYPE_OUTPUT,
                SUCCESSFUL_FLAGSTAT_OUTPUT);
    }

    @Test
    public void addsCompleteStagesToFinalPatientReport() throws Exception {

        TestReportComponent alignerComponent = new TestReportComponent();
        TestReportComponent metricsComponent = new TestReportComponent();
        TestReportComponent germlineComponent = new TestReportComponent();
        TestReportComponent snpgenotypeComponent = new TestReportComponent();
        TestReportComponent flagstatComponent = new TestReportComponent();

        AlignmentOutput alignmentWithReportComponents =
                AlignmentOutput.builder().from(SUCCESSFUL_ALIGNMENT_OUTPUT).addReportComponents(alignerComponent).build();
        when(aligner.run(REFERENCE_METADATA)).thenReturn(alignmentWithReportComponents);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(BamMetricsOutput.builder()
                .from(alignmentWithReportComponents)
                .addReportComponents(metricsComponent)
                .sample(REFERENCE)
                .build());
   *//*     when(germlineCaller.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(GermlineCallerOutput.builder()
                .from(SUCCESSFUL_GERMLINE_OUTPUT)
                .addReportComponents(germlineComponent)
                .build());*//*
        when(stageRunner.run(REFERENCE_METADATA, any())).thenReturn(SnpGenotypeOutput.builder()
                .from(SUCCESSFUL_SNPGENOTYPE_OUTPUT)
                .addReportComponents(snpgenotypeComponent)
                .build());
       *//* when(flagstat.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(FlagstatOutput.builder()
                .from(SUCCESSFUL_FLAGSTAT_OUTPUT)
                .addReportComponents(flagstatComponent)
                .build());*//*

        victim.run(REFERENCE_METADATA);
        assertThat(alignerComponent.isAdded()).isTrue();
        assertThat(metricsComponent.isAdded()).isTrue();
        assertThat(germlineComponent.isAdded()).isTrue();
        assertThat(snpgenotypeComponent.isAdded()).isTrue();
        assertThat(flagstatComponent.isAdded()).isTrue();
    }

    @Test
    public void notifiesMetadataApiWhenAlignmentComplete() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        PipelineState result = victim.run(REFERENCE_METADATA);
        verify(eventListener, times(1)).alignmentComplete(result);
    }

    @Test
    public void notifiesMetadataApiWhenPipelineComplete() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_BAM_METRICS);
   //     when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(stageRunner.run(eq(REFERENCE_METADATA), any())).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState result = victim.run(REFERENCE_METADATA);
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

    public PipelineState failed() {
        PipelineState state = mock(PipelineState.class);
        when(state.status()).thenReturn(PipelineStatus.FAILED);
        return state;
    }*/
}