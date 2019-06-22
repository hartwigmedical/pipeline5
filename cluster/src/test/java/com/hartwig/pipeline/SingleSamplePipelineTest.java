package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.alignment.after.metrics.BamMetrics;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.alignment.after.metrics.ImmutableBamMetricsOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.germline.ImmutableGermlineCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.flagstat.ImmutableFlagstatOutput;
import com.hartwig.pipeline.metadata.ImmutableSingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.LocalSampleMetadataApi;
import com.hartwig.pipeline.metadata.SampleMetadataApi;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.results.PatientReportProvider;
import com.hartwig.pipeline.results.PipelineResults;
import com.hartwig.pipeline.results.ReportComponent;
import com.hartwig.pipeline.snpgenotype.ImmutableSnpGenotypeOutput;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.testsupport.TestSamples;

import org.junit.Before;
import org.junit.Test;

public class SingleSamplePipelineTest {

    private static final String REFERENCE = "TESTR";
    private static final ImmutableSingleSampleRunMetadata REFERENCE_METADATA =
            SingleSampleRunMetadata.builder().sampleId(REFERENCE).sampleName(REFERENCE).type(SingleSampleRunMetadata.SampleType.REFERENCE).build();
    private static final SnpGenotypeOutput SUCCESSFUL_SNPGENOTYPE_OUTPUT =
            SnpGenotypeOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final GermlineCallerOutput SUCCESSFUL_GERMLINE_OUTPUT =
            GermlineCallerOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final ImmutableFlagstatOutput SUCCESSFUL_FLAGSTAT_OUTPUT =
            FlagstatOutput.builder().status(PipelineStatus.SUCCESS).build();
    private static final BamMetricsOutput SUCCESSFUL_BAM_METRICS =
            BamMetricsOutput.builder().status(PipelineStatus.SUCCESS).sample(REFERENCE).build();
    private static final ImmutableAlignmentOutput SUCCESSFUL_ALIGNMENT_OUTPUT =
            AlignmentOutput.builder().status(PipelineStatus.SUCCESS).sample(REFERENCE).build();
    public static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final String TUMOR = "TESTT";
    private SingleSamplePipeline victim;
    private Aligner aligner;
    private BamMetrics bamMetrics;
    private GermlineCaller germlineCaller;
    private SnpGenotype snpGenotype;
    private Flagstat flagstat;
    private SampleMetadataApi sampleMetadataApi;

    @Before
    public void setUp() throws Exception {
        aligner = mock(Aligner.class);
        bamMetrics = mock(BamMetrics.class);
        germlineCaller = mock(GermlineCaller.class);
        snpGenotype = mock(SnpGenotype.class);
        flagstat = mock(Flagstat.class);
        sampleMetadataApi = mock(LocalSampleMetadataApi.class);
        when(sampleMetadataApi.get()).thenReturn(REFERENCE_METADATA);
        Storage storage = mock(Storage.class);
        Bucket reportBucket = mock(Bucket.class);
        when(storage.get(ARGUMENTS.patientReportBucket())).thenReturn(reportBucket);
        final PipelineResults pipelineResults = PatientReportProvider.from(storage, ARGUMENTS).get();
        victim = new SingleSamplePipeline(sampleMetadataApi,
                aligner,
                bamMetrics,
                germlineCaller,
                snpGenotype,
                flagstat,
                pipelineResults,
                Executors.newSingleThreadExecutor(),
                ARGUMENTS);
    }

    @Test
    public void returnsFailedPipelineRunWhenAlignerStageFail() throws Exception {
        ImmutableAlignmentOutput alignmentOutput =
                AlignmentOutput.builder().status(PipelineStatus.FAILED).sample(TestSamples.simpleReferenceSample().name()).build();
        when(aligner.run(REFERENCE_METADATA)).thenReturn(alignmentOutput);
        PipelineState runOutput = victim.run();
        assertFailed(runOutput);
        assertThat(runOutput.stageOutputs()).containsExactly(alignmentOutput);
    }

    @Test
    public void returnsFailedPipelineRunWhenFlagstatStageFail() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        FlagstatOutput flagstatOutput = FlagstatOutput.builder().status(PipelineStatus.FAILED).build();
        when(bamMetrics.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(flagstatOutput);
        PipelineState runOutput = victim.run();
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
        when(bamMetrics.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(snpGenotypeOutput);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run();
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
        when(bamMetrics.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(bamMetricsOutput);
        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState runOutput = victim.run();
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
        when(bamMetrics.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_BAM_METRICS);
        ImmutableGermlineCallerOutput germlineCallerOutput = GermlineCallerOutput.builder().status(PipelineStatus.FAILED).build();
        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(germlineCallerOutput);
        when(snpGenotype.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run();
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
        when(bamMetrics.run(REFERENCE_METADATA,SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_BAM_METRICS);
        when(germlineCaller.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(REFERENCE_METADATA, SUCCESSFUL_ALIGNMENT_OUTPUT)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run();
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
        when(sampleMetadataApi.get()).thenReturn(tumorMetadata);
        AlignmentOutput tumorAlignment = AlignmentOutput.builder().from(SUCCESSFUL_ALIGNMENT_OUTPUT).sample(TUMOR).build();
        when(aligner.run(tumorMetadata)).thenReturn(tumorAlignment);
        BamMetricsOutput tumorMetrics = BamMetricsOutput.builder().from(SUCCESSFUL_BAM_METRICS).sample(TUMOR).build();
        when(bamMetrics.run(tumorMetadata, tumorAlignment)).thenReturn(tumorMetrics);
        when(germlineCaller.run(tumorMetadata, tumorAlignment)).thenReturn(SUCCESSFUL_GERMLINE_OUTPUT);
        when(snpGenotype.run(tumorMetadata, tumorAlignment)).thenReturn(SUCCESSFUL_SNPGENOTYPE_OUTPUT);
        when(flagstat.run(tumorMetadata, tumorAlignment)).thenReturn(SUCCESSFUL_FLAGSTAT_OUTPUT);
        PipelineState state = victim.run();
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
        when(bamMetrics.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(BamMetricsOutput.builder()
                .from(alignmentWithReportComponents)
                .addReportComponents(metricsComponent)
                .sample(REFERENCE)
                .build());
        when(germlineCaller.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(GermlineCallerOutput.builder()
                .from(SUCCESSFUL_GERMLINE_OUTPUT)
                .addReportComponents(germlineComponent)
                .build());
        when(snpGenotype.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(SnpGenotypeOutput.builder()
                .from(SUCCESSFUL_SNPGENOTYPE_OUTPUT)
                .addReportComponents(snpgenotypeComponent)
                .build());
        when(flagstat.run(REFERENCE_METADATA, alignmentWithReportComponents)).thenReturn(FlagstatOutput.builder()
                .from(SUCCESSFUL_FLAGSTAT_OUTPUT)
                .addReportComponents(flagstatComponent)
                .build());

        victim.run();
        assertThat(alignerComponent.isAdded()).isTrue();
        assertThat(metricsComponent.isAdded()).isTrue();
        assertThat(germlineComponent.isAdded()).isTrue();
        assertThat(snpgenotypeComponent.isAdded()).isTrue();
        assertThat(flagstatComponent.isAdded()).isTrue();
    }

    @Test
    public void notifiesMetadataApiWhenAlignmentCompleteSuccessfully() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(SUCCESSFUL_ALIGNMENT_OUTPUT);
        victim.run();
        verify(sampleMetadataApi, times(1)).complete(PipelineStatus.SUCCESS);
    }

    @Test
    public void notifiesMetadataApiWhenAlignmentFails() throws Exception {
        when(aligner.run(REFERENCE_METADATA)).thenReturn(AlignmentOutput.builder().status(PipelineStatus.FAILED).sample(REFERENCE).build());
        victim.run();
        verify(sampleMetadataApi, times(1)).complete(PipelineStatus.FAILED);
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