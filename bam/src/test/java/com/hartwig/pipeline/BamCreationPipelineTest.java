package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamIndexPipeline;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class BamCreationPipelineTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "TEST").build();
    private static final InputOutput<AlignmentRecordDataset> ALIGNED_BAM =
            InputOutput.of(OutputType.ALIGNED, SAMPLE, mock(AlignmentRecordDataset.class));
    private static final InputOutput<AlignmentRecordDataset> ENRICHED_BAM =
            InputOutput.of(OutputType.INDEL_REALIGNED, SAMPLE, mock(AlignmentRecordDataset.class));
    private static final InputOutput<AlignmentRecordDataset> FINAL_BAM =
            InputOutput.of(OutputType.FINAL, SAMPLE, mock(AlignmentRecordDataset.class));
    private InputOutput<AlignmentRecordDataset> lastStored;
    private StatusReporter.Status lastStatus;
    private List<Metric> metricsStored;
    private Monitor monitor = metric -> metricsStored.add(metric);
    private BamIndexPipeline indexer = mock(BamIndexPipeline.class);

    @Before
    public void setUp() throws Exception {
        metricsStored = new ArrayList<>();
    }

    @Test
    public void onlyDoesQCWhenBAMExists() {
        BamCreationPipeline victim = createPipeline(true, QCResult.ok());
        victim.execute(SAMPLE);
        assertThat(lastStatus).isEqualTo(StatusReporter.Status.SUCCESS);
        assertThat(lastStored).isNull();
    }

    @Test
    public void storesEnrichedBamWhenAllQCsPassAndNoErrors() {
        BamCreationPipeline victim = createPipeline(false, QCResult.ok());
        victim.execute(SAMPLE);
        assertThat(lastStatus).isEqualTo(StatusReporter.Status.SUCCESS);
        assertThat(lastStored).isEqualTo(ENRICHED_BAM);
    }

    @Test
    public void storesEnrichedBamWithFinalQCFailures() {
        BamCreationPipeline victim = createPipeline(false, QCResult.failure("final"));
        victim.execute(SAMPLE);
        assertThat(lastStatus).isEqualTo(StatusReporter.Status.FAILED_FINAL_QC);
        assertThat(lastStored).isEqualTo(ENRICHED_BAM);
    }

    @Test
    public void metricsStoredForFinalTimeSpent() {
        BamCreationPipeline victim = createPipeline(false, QCResult.ok());
        victim.execute(SAMPLE);
        assertThat(metricsStored).hasSize(1);
        assertThat(metricsStored.get(0).name()).contains(BamCreationPipeline.BAM_CREATED_METRIC);
    }

    @NotNull
    private ImmutableBamCreationPipeline createPipeline(final boolean exists, QCResult finalQC) {
        return BamCreationPipeline.builder()
                .alignment(input -> ALIGNED_BAM)
                .bamEnrichment(enrichment())
                .finalBamStore(finalStore(exists))
                .finalDatasource(sample -> FINAL_BAM)
                .finalQC(toQC -> finalQC).statusReporter(status -> lastStatus = status).indexBam(indexer).monitor(monitor)
                .build();
    }

    @NotNull
    private OutputStore<AlignmentRecordDataset> finalStore(final boolean exists) {
        return new OutputStore<AlignmentRecordDataset>() {
            @Override
            public void store(final InputOutput<AlignmentRecordDataset> inputOutput) {
                lastStored = inputOutput;
            }

            @Override
            public boolean exists(final Sample sample, final OutputType type) {
                return exists;
            }
        };
    }

    @NotNull
    private Stage<AlignmentRecordDataset, AlignmentRecordDataset> enrichment() {
        return new Stage<AlignmentRecordDataset, AlignmentRecordDataset>() {
            @Override
            public OutputType outputType() {
                return OutputType.INDEL_REALIGNED;
            }

            @Override
            public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) throws IOException {
                return ENRICHED_BAM;
            }
        };
    }

}